package opi

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/chmduquesne/rollinghash/adler32"
	"github.com/golang/snappy"
)

type any interface{}

const (
	// splitting algorithm
	fanout     = 4
	chunkbits  = 13
	windowSize = 128

	chunkmask = 0xffffffff >> (32 - chunkbits)            // boundary of a chunk
	topmask   = 0xffffffff >> ((32 - chunkbits) % fanout) // boundary of the top level
)

func Slice(path string) []byte {
	fmt.Printf("splitting %s\n", path)
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	r := bufio.NewReader(f)
	_, id, _, err := SliceUntil(r, topmask)
	return id
}

func store(b []byte) []byte {
	encoded := snappy.Encode(nil, b)
	res := fmt.Sprintf("%x", sha1.Sum(encoded))
	fmt.Printf("Storing %s\n", res)
	return []byte(res)
}

// Read the buffer until one of these conditions is met:
// - The rolling checksum matches the mask
// - The end of the buffer is reached
// Exception: when the input mask is topmask, the read does not stop
// until the end of the buffer
//
// Store the resulting intermediate metachunks, and return
// - n the number of bytes consumed
// - id the address of the stored metachunk
// - rollsum the rolling checksum at the end of the metachunk
// - err the error indicating whether the end of the buffer was reached
func SliceUntil(r *bufio.Reader, mask uint32) (n uint64, id []byte, rollsum uint32, err error) {
	if mask > chunkmask {
		s := NewSuperChunk()
		offset := uint64(0)
		for {
			n, id, rollsum, err := SliceUntil(r, mask>>fanout)
			metaType := byte('S')
			if mask>>fanout == chunkmask {
				metaType = byte('C')
			}
			s.AddChild(offset, metaType, id)
			if ((rollsum&mask == mask) && mask < topmask) || err != nil {
				id = store(s.Bytes())
				return offset, id, rollsum, err
			}
			offset += n
		}
	} else {
		// initially 128 bytes, capacity 4 * 8192
		data := make([]byte, windowSize, 4*(1<<chunkbits))
		hash := adler32.New()

		// read the initial window
		n, err := r.Read(data)
		if err != nil {
			// we read the file to its end, check if it had data
			if err == io.EOF && n > 0 {
				data = data[:n]
				hash.Write(data)
				return uint64(n), store(data), hash.Sum32(), err
			}
			return 0, []byte(""), 0, err
		}
		hash.Write(data)
		for hash.Sum32()&mask != mask {
			b, err := r.ReadByte()
			if err != nil {
				break
			}
			n += 1
			hash.Roll(b)
			data = append(data, b)
		}
		return uint64(n), store(data), hash.Sum32(), err
	}
}

func Archive(path string) []byte {
	info, err := os.Lstat(path)
	if err != nil {
		log.Fatal(err)
	}
	var res []byte
	if info.IsDir() {
		files, err := ioutil.ReadDir(path)
		if err != nil {
			log.Fatal(err)
		}
		entries := make([]string, 0)
		for _, f := range files {
			s := Archive(path + "/" + f.Name())
			entries = append(entries, string(s))
		}
		resb, err := json.Marshal(entries)
		if err != nil {
			log.Fatal(err)
		}
		res = store(resb)
	} else {
		res = Slice(path)
	}
	fmt.Printf("%s -> %s\n", path, res)
	return res
}
