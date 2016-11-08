package opi

import (
	"bufio"
	"crypto/sha512"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/chmduquesne/rollinghash/adler32"
	"github.com/cloudfoundry/bytefmt"
)

const (
	// splitting algorithm
	fanout     = 4
	chunkBits  = 13
	windowSize = 128

	// Dependent values
	maxChunkSize = 1 << (chunkBits + 3)                      // see maxChunkSize.md
	chunkMask    = 0xffffffff >> (32 - chunkBits)            // boundary of a chunk
	topMask      = 0xffffffff >> ((32 - chunkBits) % fanout) // boundary of the top level
)

type Opi struct {
	Store Storage
	Codec Codec
}

func NewOpi(s Storage, c Codec) Timeline {
	return &Opi{Store: s, Codec: c}
}

func (o *Opi) Slice(path string) (addr []byte, filetype byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	stream := bufio.NewReader(f)
	n, addr, filetype, _, err := o.SliceUntil(stream, topMask)
	fmt.Printf("%s (%v) -> %s\n", path, bytefmt.ByteSize(n), addr)
	return addr, filetype, err
}

func (o *Opi) Save(b []byte) []byte {
	//value := snappy.Encode(nil, b)
	value := b
	hash := []byte(fmt.Sprintf("%x", sha512.Sum512(value)))

	//fmt.Println(string(hash))
	o.Store.Set(hash, value)
	return hash
}

// Read the buffer until one of these conditions is met:
// - The rolling checksum matches the mask
// - The end of the buffer is reached
// Exception: when the input mask is topMask, the read does not stop
// until the end of the buffer
//
// Store the resulting intermediate metachunks, and return
// - n the number of bytes consumed from the buffer
// - addr the address of the stored metachunk
// - metatype the type of the stored metachunk ('C' or 'S')
// - rollsum the rolling checksum at the end of the metachunk
// - err the error indicating whether the end of the buffer was reached
func (o *Opi) SliceUntil(stream *bufio.Reader, mask uint32) (n uint64, addr []byte, metatype byte, rollsum uint32, err error) {
	if mask > chunkMask {
		s := NewSuperChunk()
		offset := uint64(0)
		for {
			n, addr, metatype, rollsum, err = o.SliceUntil(stream, mask>>fanout)
			s.AddChild(offset, metatype, addr)
			offset += n
			if ((rollsum&mask == mask) && mask < topMask) || err != nil {
				// If we have exactly 1 child, return it directly
				if len(s.Children) == 1 {
					return
				}
				return offset, o.Save(o.Codec.Encode(s.toGoObj())), byte('S'), rollsum, err
			}
		}
	} else {
		// Chunk
		data := make([]byte, windowSize, maxChunkSize)
		roll := adler32.New()

		// read the initial window
		sz := 0
		sz, err = stream.Read(data)
		n = uint64(sz)

		// EOF during initial window
		if err != nil {
			if err == io.EOF {
				data = data[:n]
				rollsum := uint32(0)
				if n > 0 {
					roll.Write(data)
					rollsum = roll.Sum32()
				}
				return n, o.Save(data), byte('C'), rollsum, err
			}
			return
		}
		// Roll until boundary or EOF
		roll.Write(data)
		for roll.Sum32()&mask != mask && n < maxChunkSize {
			b, err := stream.ReadByte()
			if err != nil {
				break
			}
			n += 1
			roll.Roll(b)
			data = append(data, b)
		}
		return uint64(n), o.Save(data), 0, roll.Sum32(), err
	}
}

func (o *Opi) Snapshot(path string) (addr []byte, filetype byte, err error) {
	info, err := os.Lstat(path)
	if err != nil {
		log.Fatal(err)
	}
	switch {
	case info.Mode()&os.ModeType == os.ModeDir:
		files, err := ioutil.ReadDir(path)
		if err != nil {
			log.Fatal(err)
		}
		d := NewDir()
		for _, f := range files {
			path := path + "/" + f.Name()
			info, err := os.Lstat(path)
			if err != nil {
				log.Fatal(err)
			}
			addr, filetype, err := o.Snapshot(path + "/" + f.Name())
			if err != nil {
				log.Fatal(err)
			}
			d.AddEntry(filetype, uint32(info.Mode()&os.ModePerm), []byte(f.Name()), nil, addr)
		}
		return o.Save(o.Codec.Encode(d.toGoObj())), byte('d'), err
	case info.Mode()&os.ModeType == os.ModeSymlink:
		target, err := os.Readlink(path)
		if err != nil {
			log.Fatal(err)
		}
		return o.Save([]byte(target)), byte('l'), err
	case info.Mode()&os.ModeType == 0:
		return o.Slice(path)
	default:
		fmt.Printf("%s: file type not supported\n", path)
	}
	return
}

func (o *Opi) Archive(path string, name string) error {
	o.Snapshot(path)
	return nil
}

func (o *Opi) Restore(name string, path string) error {
	return nil
}
