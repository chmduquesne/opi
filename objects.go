package opi

import (
	"errors"
	"fmt"
	"time"
)

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
	Host    []byte
	Replica []byte
	Parents [][]byte
}

func (c *Commit) toGoObj() interface{} {
	return [5]interface{}{
		c.Date.Format(time.UnixDate),
		c.Tree,
		c.Host,
		c.Replica,
		c.Parents,
	}
}

func NewCommit(date time.Time, tree []byte, host []byte, replica []byte, parents [][]byte) *Commit {
	return &Commit{
		Date:    date,
		Tree:    tree,
		Host:    host,
		Replica: replica,
		Parents: parents,
	}
}

func ReadCommit(obj interface{}) (*Commit, error) {
	data, ok := obj.([]interface{})
	if !ok {
		fmt.Print(obj)
		return nil, errors.New("Could not parse commit")
	}
	if len(data) != 5 {
		return nil, errors.New("Commit does not have the right number of fields")
	}
	date, ok := data[0].(string)
	if !ok {
		return nil, errors.New("Could not get date as string")
	}
	commitDate, err := time.Parse(time.UnixDate, date)
	if err != nil {
		return nil, err
	}
	tree, ok := data[1].(string)
	if !ok {
		return nil, errors.New("Could not get tree as string")
	}
	host, ok := data[2].(string)
	if !ok {
		return nil, errors.New("Could not get host as string")
	}
	c := &Commit{
		Date:    commitDate,
		Tree:    []byte(tree),
		Host:    []byte(host),
		Replica: nil,
		Parents: nil,
	}

	return c, nil
}

func ReadAddr(obj interface{}) ([]byte, error) {
	addr_str, ok := obj.(string)
	if !ok {
		return nil, errors.New("Could not read address")
	}
	return []byte(addr_str), nil
}
