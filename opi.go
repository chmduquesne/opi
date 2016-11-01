package opi

import (
	"bufio"
	"crypto/sha512"
	"encoding/json"
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
}

func NewOpi(s Storage) Timeline {
	return &Opi{Store: s}
}

func (o *Opi) Slice(path string) []byte {
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
	n, id, _, _, err := o.SliceUntil(stream, topMask)
	fmt.Printf("%s (%v) -> %s\n", path, bytefmt.ByteSize(n), id)
	return id
}

func (o *Opi) Save(b []byte) []byte {
	//value := snappy.Encode(nil, b)
	value := b
	hash := []byte(fmt.Sprintf("%x", sha512.Sum512(value)))

	//fmt.Println(string(hash))
	//o.Store.Set(hash, value)
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
				return offset, o.Save(s.Bytes()), byte('S'), rollsum, err
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

func (o *Opi) SliceAll(path string) []byte {
	info, err := os.Lstat(path)
	if err != nil {
		log.Fatal(err)
	}
	var res []byte
	switch {
	case info.Mode()&os.ModeType == os.ModeDir:
		files, err := ioutil.ReadDir(path)
		if err != nil {
			log.Fatal(err)
		}
		entries := make([]string, 0)
		for _, f := range files {
			s := o.SliceAll(path + "/" + f.Name())
			entries = append(entries, string(s))
		}
		resb, err := json.Marshal(entries)
		if err != nil {
			log.Fatal(err)
		}
		res = o.Save(resb)
	case info.Mode()&os.ModeType == os.ModeSymlink:
		fmt.Printf("symlink")
	case info.Mode()&os.ModeType == 0:
		res = o.Slice(path)
	default:
		fmt.Printf("%s: file type not supported\n", path)
	}
	return res
}

func (o *Opi) Archive(path string, name string) error {
	o.SliceAll(path)
	return nil
}

func (o *Opi) Restore(name string, path string) error {
	return nil
}
