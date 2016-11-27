package opi

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

func ChunksEqual(c1, c2 *Chunk) bool {
	if c1 == nil || c2 == nil {
		return false
	}
	return bytes.Equal(c1.Data, c2.Data)
}

func TestChunk(t *testing.T) {
	inputs := [][]byte{
		nil,
		[]byte("hello"),
	}
	for _, in := range inputs {
		c := NewChunk(in)
		b, err := c.Bytes()
		if err != nil {
			t.Fatal("Unexpected error when converting Chunk to byte array\n")
		}
		readRes, err := ReadChunkBytes(b)
		if err != nil {
			t.Fatal("Unexpected error when converting byte array to Chunk\n")
		}
		if !ChunksEqual(c, readRes) {
			t.Fatal("Incorrect back and forth convertion\n")
		}
	}
}

var rnd = rand.New(rand.NewSource(0))

func RandomBytes() (res []byte) {
	val, _ := quick.Value(reflect.TypeOf(res), rnd)
	return val.Interface().([]byte)
}

func TestChunkBlackBox(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := NewChunk(RandomBytes())
		b, err := c.Bytes()
		if err != nil {
			t.Fatal("Unexpected error when converting Chunk to byte array\n")
		}
		readRes, err := ReadChunkBytes(b)
		if err != nil {
			t.Fatal("Unexpected error when converting byte array to Chunk\n")
		}
		if !ChunksEqual(c, readRes) {
			t.Fatal("Incorrect back and forth convertion\n")
		}
	}
}

func SuperChunksEqual(s1, s2 *SuperChunk) bool {
	if s1 == nil || s2 == nil {
		return false
	}
	if len(s1.Children) != len(s2.Children) {
		return false
	}
	for i, _ := range s1.Children {
		c1, c2 := s1.Children[i], s2.Children[i]
		if !bytes.Equal(c1.Addr, c2.Addr) {
			return false
		}
		if c1.MetaType != c2.MetaType {
			return false
		}
		if c1.Offset != c2.Offset {
			return false
		}
	}
	return true
}

func TestSuperChunk(t *testing.T) {
}
