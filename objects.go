package opi

import (
	"bytes"

	bencode "github.com/jackpal/bencode-go"
)

type Any interface{}

type FSObject interface {
	Bytes() []byte
}

// Chunk

type Chunk struct {
	data []byte
}

func (c *Chunk) Bytes() []byte {
	return c.data
}

func ReadChunk(from []byte) *Chunk {
	return &Chunk{data: from}
}

func NewChunk() *Chunk {
	return &Chunk{}
}

// SuperChunk

type MetaChunkInfo struct {
	Offset   uint64
	MetaType byte
	Addr     []byte
	// Not serialized
	Size    uint64
	RollSum uint32
}

type SuperChunk struct {
	Children []MetaChunkInfo
}

func (s *SuperChunk) AddChild(offset uint64, metaType byte, addr []byte) {
	m := MetaChunkInfo{
		Offset:   offset,
		MetaType: metaType,
		Addr:     addr,
	}
	s.Children = append(s.Children, m)
}

func (s *SuperChunk) Bytes() []byte {
	var buf bytes.Buffer
	var compacted [][3]interface{}
	for _, c := range s.Children {
		compacted = append(compacted, [3]interface{}{c.Offset, c.MetaType, c.Addr})
	}
	bencode.Marshal(&buf, compacted)
	return buf.Bytes()
}

func ReadSuperChunk(from []byte) *SuperChunk {
	buf := bytes.NewReader(from)
	var raw [][3]interface{}
	bencode.Unmarshal(buf, &raw)
	s := SuperChunk{}
	for _, r := range raw {
		s.AddChild(uint64(r[0].(int64)), byte(r[1].(int64)), []byte(r[2].(string)))
	}
	return &s
}

func NewSuperChunk() *SuperChunk {
	return &SuperChunk{}
}
