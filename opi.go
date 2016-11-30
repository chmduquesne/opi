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

	"github.com/cloudfoundry/bytefmt"
	adler32 "gopkg.in/chmduquesne/rollinghash.v1/adler32"
)

const (
	// splitting algorithm
	fanout     = 4
	chunkBits  = 13
	windowSize = 256
	maxWriters = 1000

	// Dependent values
	maxChunkSize = 1 << (chunkBits + 5)                      // see maxChunkSize.md
	chunkMask    = 0xffffffff >> (32 - chunkBits)            // boundary of a chunk
	topMask      = 0xffffffff >> ((32 - chunkBits) % fanout) // boundary of the top level
)

type Opi struct {
	Storage
	Codec
	writers chan bool
}

func NewOpi(s Storage, c Codec) Timeline {
	return &Opi{
		Storage: s,
		Codec:   c,
		writers: make(chan bool, maxWriters),
	}
}

// Wrap Storage.Set to do things concurrently. If an error occurs, a panic
// is generated and must be recovered by the caller in order to exit
// cleanly.
func (o *Opi) Set(key []byte, value []byte) {
	o.writers <- true
	go func() {
		defer func() { <-o.writers }()
		if err := o.Storage.Set(key, value); err != nil {
			panic(err)
		}
	}()
}

func (o *Opi) Serialize(f FSObject) ([]byte, error) {
	value, err := f.Bytes()
	if err != nil {
		return nil, err
	}
	addr := []byte(fmt.Sprintf("%x", sha512.Sum512(value)))
	encoded, err := o.Encode(value)
	if err != nil {
		return nil, err
	}
	o.Set(addr, encoded)
	return addr, nil
}

func (o *Opi) DeSerialize(addr []byte) (value []byte, err error) {
	encoded, err := o.Get(addr)
	if err != nil {
		return nil, err
	}
	value, err = o.Decode(encoded)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (o *Opi) Slice(path string) (addr []byte, filetype byte, err error) {
	var f *os.File
	if f, err = os.Open(path); err != nil {
		return
	}
	defer func() {
		if errClose := f.Close(); err == nil {
			err = errClose
		}
	}()
	stream := bufio.NewReader(f)
	n, addr, filetype, _, err := o.SliceUntil(stream, topMask)
	fmt.Printf("%s (%v)\n", path, bytefmt.ByteSize(n))
	if err == io.EOF {
		err = nil
	}
	return addr, filetype, err
}

func (o *Opi) SliceUntil(stream *bufio.Reader, mask uint32) (n uint64, addr []byte, metatype byte, rollsum uint32, err error) {
	var errSerialize error
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
				addr, errSerialize = o.Serialize(s)
				if err == nil {
					err = errSerialize
				}
				return offset, addr, byte('S'), rollsum, err
			}
		}
	}
	// else
	n, addr, metatype, rollsum, err = o.Chunk(stream)
	fmt.Println(bytefmt.ByteSize(n))
	return
}

func (o *Opi) Chunk(stream *bufio.Reader) (n uint64, addr []byte, metatype byte, rollsum uint32, err error) {
	var errSerialize error

	data := make([]byte, windowSize, maxChunkSize)
	roll := adler32.New()

	// read the initial window
	sz := 0
	sz, err = stream.Read(data)
	data = data[:sz]
	n = uint64(sz)

	// EOF during initial window
	if err != nil {
		if err == io.EOF {
			rollsum := uint32(0)
			if n > 0 {
				roll.Write(data)
				rollsum = roll.Sum32()
			}
			c := NewChunk(data)
			addr, errSerialize = o.Serialize(c)
			if err == nil {
				err = errSerialize
			}
			return n, addr, byte('C'), rollsum, err
		}
		return
	}
	// Roll until boundary or EOF
	roll.Write(data)
	var b byte
	for roll.Sum32()&chunkMask != chunkMask && n < maxChunkSize {
		b, err = stream.ReadByte()
		if err != nil {
			break
		}
		n += 1
		roll.Roll(b)
		data = append(data, b)
	}
	c := NewChunk(data)
	addr, errSerialize = o.Serialize(c)
	if err == nil {
		err = errSerialize
	}
	return uint64(n), addr, byte('C'), roll.Sum32(), err
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
	defer func() {
		if err := recover(); err != nil {
			log.Fatal(err)
		}
	}()
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
	encodedAddr, err := o.Encode(addr)
	if err != nil {
		return err
	}
	o.Set([]byte(name), encodedAddr)
	if err != nil {
		return err
	}
	return nil
}

func (o *Opi) Restore(name string, path string) error {
	// address of the top commit
	encodedAddr, err := o.Get([]byte(name))
	if err != nil {
		return err
	}
	addr, err := o.Decode(encodedAddr)
	if err != nil {
		return err
	}
	// top commit
	b, err := o.DeSerialize(addr)
	if err != nil {
		return err
	}
	c, err := ReadCommit(b)
	if err != nil {
		return err
	}
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
	b, err := o.DeSerialize(addr)
	if err != nil {
		return err
	}
	d, err := ReadDir(b)
	if err != nil {
		return err
	}
	for _, e := range d.Entries {
		name := dest + "/" + string(e.Name)
		_, err := os.Lstat(name)
		if err == nil {
			return errors.New("Destination already exists")
		}
		switch {
		case e.FileType == byte('d'):
			os.Mkdir(name, 0777)
			o.Rebuild(e.Addr, name)
		case e.FileType == byte('l'):
			b, err := o.DeSerialize(e.Addr)
			if err != nil {
				return err
			}
			s, err := ReadSymlink(b)
			if err != nil {
				return err
			}
			os.Symlink(s.Target, name)
		case e.FileType == byte('S') || e.FileType == byte('C'):
			var f *os.File
			if f, err = os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666); err != nil {
				return err
			}
			defer func() {
				if closingErr := f.Close(); err == nil {
					err = closingErr
				}
			}()
			stream := bufio.NewWriter(f)
			if e.FileType == byte('S') {
				if err = o.Glue(e.Addr, stream); err != nil {
					return err
				}
			} else {
				if err = o.WriteChunk(e.Addr, stream); err != nil {
					return err
				}
			}
			if err = stream.Flush(); err != nil {
				return err
			}
		}
		os.Chmod(name, os.FileMode(e.Mode))
		fmt.Println(name)
	}
	return nil
}

func (o *Opi) WriteChunk(addr []byte, stream io.Writer) (err error) {
	b, err := o.DeSerialize(addr)
	if err != nil {
		return err
	}
	c, err := ReadChunk(b)
	if err != nil {
		return err
	}
	_, err = stream.Write(c.Data)
	return err
}

func (o *Opi) Glue(addr []byte, stream io.Writer) (err error) {
	b, err := o.DeSerialize(addr)
	if err != nil {
		return err
	}
	s, err := ReadSuperChunk(b)
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
