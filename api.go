package opi

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/chmduquesne/rollinghash/adler32"
	"github.com/golang/snappy"
	bencode "github.com/jackpal/bencode-go"
)

type any interface{}

const (
	// splitting algorithm
	fanout    = 4
	chunkbits = 13
	levelmax  = (32 - chunkbits) / fanout
)

func boundaryMask(level int) uint32 {
	return 0xffffffff >> uint32(32-(chunkbits+level*fanout))
}

func sliceFile(path string) string {

	fmt.Printf("splitting %s\n", path)

	fi, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	bufreader := bufio.NewReader(fi)

	_, id, _, err := doSlice(bufreader, levelmax)

	return id

}

func store(b []byte) string {
	encoded := snappy.Encode(nil, b)
	res := fmt.Sprintf("%x", sha1.Sum(encoded))
	fmt.Printf("Storing %s\n", res)
	return res
}

func doSlice(r *bufio.Reader, level int) (rollsum uint32, id string, n int, err error) {

	m := boundaryMask(level)
	isBoundary := func(rollsum uint32) bool { return rollsum&m == m }

	if level > 0 {

		entries := make([][3]any, 0)
		//entries := make(map[int]string)
		offset := 0
		for {
			rollsum, id, n, err := doSlice(r, level-1)
			entries = append(entries, [3]any{id, n, "S"})
			//entries[offset] = id
			offset += n
			if (isBoundary(rollsum) && level < levelmax) || err != nil {
				var buf bytes.Buffer
				bencode.Marshal(&buf, entries)
				resb := buf.Bytes()
				println(string(resb))
				//resb, _ := json.Marshal(entries)
				id = store(resb)
				return rollsum, id, offset, err
			}
		}
	} else {
		data := make([]byte, 128, 4*(1<<chunkbits))
		hash := adler32.New()

		n, err := r.Read(data)
		if err != nil {
			if err == io.EOF && n > 0 {
				data = data[:n]
				hash.Write(data)
				return hash.Sum32(), store(data), n, err
			}
			return 0, "", 0, err
		}
		hash.Write(data)
		for !isBoundary(hash.Sum32()) {
			b, err := r.ReadByte()
			if err != nil {
				break
			}
			hash.Roll(b)
			data = append(data, b)
		}
		return hash.Sum32(), store(data), n, err
	}

}

func slice(path string) string {
	info, err := os.Lstat(path)
	if err != nil {
		log.Fatal(err)
	}

	var res string

	if info.IsDir() {
		files, err := ioutil.ReadDir(path)
		if err != nil {
			log.Fatal(err)
		}

		entries := make([]string, 0)

		for _, f := range files {
			s := slice(path + "/" + f.Name())
			entries = append(entries, s)
		}

		resb, err := json.Marshal(entries)
		if err != nil {
			log.Fatal(err)
		}
		res = store(resb)
	} else {
		res = sliceFile(path)
	}

	fmt.Printf("%s -> %s\n", path, res)

	return res
}
