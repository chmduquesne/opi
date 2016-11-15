package opi

import (
	"bufio"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

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
	Storage
	Codec
}

func NewOpi(s Storage, c Codec) Timeline {
	return &Opi{Storage: s, Codec: c}
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
	if err == io.EOF {
		err = nil
	}
	return addr, filetype, err
}

func (o *Opi) Serialize(f FSObject) ([]byte, error) {
	obj := f.toGoObj()
	value, err := o.Encode(obj)
	if err != nil {
		return nil, err
	}
	addr := []byte(fmt.Sprintf("%x", sha512.Sum512(value)))
	//fmt.Println(string(addr))
	err = o.Set(addr, value)
	if err != nil {
		return nil, err
	}
	return addr, nil
}

func (o *Opi) DeSerialize(addr []byte) (obj interface{}, err error) {
	data, err := o.Get(addr)
	if err != nil {
		return nil, err
	}
	return o.Decode(data)
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
				addr, err = o.Serialize(s)
				return offset, addr, byte('S'), rollsum, err
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
				c := NewChunk(data)
				addr, _ = o.Serialize(c)
				return n, addr, byte('C'), rollsum, err
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
		c := NewChunk(data)
		addr, _ = o.Serialize(c)
		return uint64(n), addr, byte('C'), roll.Sum32(), err
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
			info, err := os.Lstat(path + "/" + f.Name())
			if err != nil {
				log.Fatal(err)
			}
			addr, filetype, err := o.Snapshot(path + "/" + f.Name())
			if err != nil {
				log.Fatal(err)
			}
			d.AddEntry(filetype, uint32(info.Mode()&os.ModePerm), []byte(f.Name()), []byte("xattr"), addr)
		}
		addr, err := o.Serialize(d)
		return addr, byte('d'), err
	case info.Mode()&os.ModeType == os.ModeSymlink:
		target, err := os.Readlink(path)
		if err != nil {
			log.Fatal(err)
		}
		s := NewSymlink(target)
		addr, err := o.Serialize(s)
		return addr, byte('l'), err
	case info.Mode()&os.ModeType == 0:
		return o.Slice(path)
	default:
		fmt.Printf("%s: file type not supported\n", path)
	}
	return
}

func (o *Opi) Archive(path string, name string) error {
	addr, filetype, err := o.Snapshot(path)
	if filetype != byte('d') {
		return errors.New("Can only archive a directory")
	}
	if err != nil {
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	c := NewCommit(time.Now(), addr, []byte(hostname), []byte(hostname), nil)
	addr, err = o.Serialize(c)
	if err != nil {
		return err
	}
	value, err := o.Encode(addr)
	if err != nil {
		return err
	}
	err = o.Set([]byte(name), value)
	if err != nil {
		return err
	}
	return nil
}

func (o *Opi) Restore(name string, path string) error {
	// address of the top commit
	obj, err := o.DeSerialize([]byte(name))
	if err != nil {
		return err
	}
	addr, err := ReadAddr(obj)
	if err != nil {
		return err
	}
	fmt.Println("Top commit address: ", string(addr))

	// top commit
	obj, err = o.DeSerialize(addr)
	if err != nil {
		return err
	}
	c, err := ReadCommit(obj)
	if err != nil {
		return err
	}
	fmt.Println("commits point to ", string(c.Tree))

	return o.Rebuild(c.Tree, path)
}

func (o *Opi) Rebuild(addr []byte, dest string) (err error) {
	// dest must exist
	info, err := os.Lstat(dest)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("Destination is not a directory")
	}
	obj, err := o.DeSerialize(addr)
	if err != nil {
		return err
	}
	d, err := ReadDir(obj)
	if err != nil {
		return err
	}
	for _, e := range d.Entries {
		name := dest + "/" + string(e.Name)
		fmt.Println(name)
		_, err := os.Lstat(name)
		if err == nil {
			return errors.New("Destination already exists")
		}
		switch {
		case e.FileType == byte('d'):
			os.Mkdir(name, os.FileMode(e.Mode))
			o.Rebuild(e.Addr, name)
		case e.FileType == byte('l'):
			obj, err := o.DeSerialize(e.Addr)
			if err != nil {
				return err
			}
			s, err := ReadSymlink(obj)
			if err != nil {
				return err
			}
			os.Symlink(s.Target, name)
		case e.FileType == byte('S'):
			f, err := os.Create(name)
			defer f.Close()
			if err != nil {
				return err
			}
			stream := bufio.NewWriter(f)
			err = o.Glue(e.Addr, stream)
			if err != nil {
				return err
			}
		case e.FileType == byte('C'):
			f, err := os.Create(name)
			defer f.Close()
			if err != nil {
				return err
			}
			stream := bufio.NewWriter(f)
			err = o.WriteChunk(e.Addr, stream)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Opi) WriteChunk(addr []byte, stream *bufio.Writer) (err error) {
	obj, err := o.DeSerialize(addr)
	if err != nil {
		return err
	}
	c, err := ReadChunk(obj)
	if err != nil {
		return err
	}
	_, err = stream.Write(c.Data)
	return err
}

func (o *Opi) Glue(addr []byte, stream *bufio.Writer) (err error) {
	obj, err := o.DeSerialize(addr)
	if err != nil {
		return err
	}
	s, err := ReadSuperChunk(obj)
	if err != nil {
		return err
	}
	for _, c := range s.Children {
		switch {
		case c.MetaType == byte('S'):
			err := o.Glue(c.Addr, stream)
			if err != nil {
				return err
			}
		case c.MetaType == byte('C'):
			err := o.WriteChunk(c.Addr, stream)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
