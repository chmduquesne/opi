package opi

import (
	"bytes"
	"testing"
)

func superChunksIdentical(s1, s2 *SuperChunk) bool {
	if len(s1.Children) != len(s2.Children) {
		return false
	}
	for i := range s1.Children {
		c1 := s1.Children[i]
		c2 := s2.Children[i]
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

func TestSuperChunkSerializeRead(t *testing.T) {
	m := MetaChunkInfo{
		Offset:   0,
		MetaType: 'C',
		Addr:     []byte("abc"),
	}
	s := SuperChunk{
		Children: []MetaChunkInfo{m},
	}
	data := s.Bytes()
	loaded := ReadSuperChunk(data)
	if !superChunksIdentical(&s, loaded) {
		t.Fatal("incorrect serialization")
	}
}
