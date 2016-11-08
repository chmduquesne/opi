package opi

import "time"

type FSObject interface {
	toGoObj() interface{}
}

// Chunk

type Chunk struct {
	data []byte
}

func (c *Chunk) toGoObj() interface{} {
	return c.data
}

func NewChunk() *Chunk {
	return &Chunk{}
}

// SuperChunk

type MetaChunkInfo struct {
	Offset   uint64
	MetaType byte
	Addr     []byte
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

func (s *SuperChunk) toGoObj() interface{} {
	var obj [][3]interface{}
	for _, c := range s.Children {
		obj = append(obj, [3]interface{}{c.Offset, c.MetaType, c.Addr})
	}
	return obj
}

func NewSuperChunk() *SuperChunk {
	return &SuperChunk{}
}

type Xattr struct {
	attributes map[string]string
}

type DirEntry struct {
	FileType byte
	Mode     uint32
	Name     []byte
	Xattr    []byte
	Addr     []byte
}

func (d *Dir) AddEntry(fileType byte, mode uint32, name []byte, xattr []byte, addr []byte) {
	entry := DirEntry{
		FileType: fileType,
		Mode:     mode,
		Name:     name,
		Xattr:    xattr,
		Addr:     addr,
	}
	d.Entries = append(d.Entries, entry)
}

type Dir struct {
	Entries []DirEntry
}

func (d *Dir) toGoObj() interface{} {
	var obj [][5]interface{}
	for _, e := range d.Entries {
		obj = append(obj, [5]interface{}{e.FileType, e.Mode, e.Name, e.Xattr, e.Addr})
	}
	return obj
}

func NewDir() *Dir {
	return &Dir{}
}

type Commit struct {
	Date    time.Time
	Tree    []byte
	Host    string
	Replica []byte
	Parents [][]byte
}
