package opi

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

func ChunksEqual(c1, c2 *Chunk) bool {
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
