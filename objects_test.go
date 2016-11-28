package opi

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"
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
			t.Fatal(err)
		}
		readRes, err := ReadChunkBytes(b)
		if err != nil {
			t.Fatal(err)
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
			t.Fatal(err)
		}
		readRes, err := ReadChunkBytes(b)
		if err != nil {
			t.Fatal(err)
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
	s := NewSuperChunk()
	s.AddChild(0, byte('C'), []byte("hello"))
	s.AddChild(10, byte('C'), []byte("world"))
	b, err := s.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	readRes, err := ReadSuperChunkBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	if !SuperChunksEqual(s, readRes) {
		t.Fatal("Incorrect back and forth convertion\n")
	}
}

func DirsEqual(d1, d2 *Dir) bool {
	if d1 == nil || d2 == nil {
		return false
	}
	if len(d1.Entries) != len(d2.Entries) {
		return false
	}
	for i, _ := range d1.Entries {
		e1, e2 := d1.Entries[i], d2.Entries[i]
		if e1.FileType != e2.FileType {
			return false
		}
		if e1.Mode != e2.Mode {
			return false
		}
		if !bytes.Equal(e1.Name, e2.Name) {
			return false
		}
		if !bytes.Equal(e1.Addr, e2.Addr) {
			return false
		}
		if !bytes.Equal(e1.Xattr, e2.Xattr) {
			return false
		}
	}
	return true
}

func TestDir(t *testing.T) {
	d := NewDir()
	d.AddEntry(
		byte('C'),
		0644,
		[]byte("small file"),
		[]byte("small file addr"),
		[]byte("small file xattr addr"),
	)
	b, err := d.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	readRes, err := ReadDirBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	if !DirsEqual(d, readRes) {
		t.Fatal("Incorrect back and forth convertion\n")
	}
}

func CommitsEqual(c1, c2 *Commit) bool {
	if c1 == nil || c2 == nil {
		return false
	}
	if !c1.Date.Equal(c2.Date) {
		return false
	}
	if !bytes.Equal(c1.Tree, c2.Tree) {
		return false
	}
	if !bytes.Equal(c1.Host, c2.Host) {
		return false
	}
	if !bytes.Equal(c1.Replica, c2.Replica) {
		return false
	}
	if len(c1.Parents) != len(c2.Parents) {
		return false
	}
	for i, _ := range c1.Parents {
		if !bytes.Equal(c1.Parents[i], c2.Parents[i]) {
			return false
		}
	}
	return true
}

func TestCommit(t *testing.T) {
	c := NewCommit(
		time.Now(),
		[]byte("tree"),
		[]byte("host"),
		[]byte("replica"),
		nil,
	)
	b, err := c.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	readRes, err := ReadCommitBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	if !CommitsEqual(c, readRes) {
		t.Fatal("Incorrect back and forth convertion\n")
	}
}
