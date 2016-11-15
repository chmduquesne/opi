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
	Data []byte
}

func (c *Chunk) toGoObj() interface{} {
	return c.Data
}

func NewChunk(data []byte) *Chunk {
	return &Chunk{Data: data}
}

func ReadChunk(obj interface{}) (c *Chunk, err error) {
	data, ok := obj.(string)
	if !ok {
		return nil, errors.New("ReadChunk: Can't parse object to a byte array")
	}
	return NewChunk([]byte(data)), nil
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

func ReadSuperChunk(obj interface{}) (s *SuperChunk, err error) {
	s = NewSuperChunk()
	data, ok := obj.([]interface{})
	if !ok {
		return nil, errors.New("ReadSuperChunk: Can't parse object to a list")
	}
	for _, c := range data {
		l := c.([]interface{})
		if len(l) != 3 {
			return nil, errors.New("ReadSuperChunk: Entry does not have 3 fields")
		}
		o, ok := l[0].(int64)
		if !ok {
			return nil, errors.New("ReadSuperChunk: Could not parse offset")
		}
		offset := uint64(o)
		m, ok := l[1].(int64)
		if !ok {
			return nil, errors.New("ReadSuperChunk: Could not parse metatype")
		}
		metaType := byte(m)
		a, ok := l[2].(string)
		if !ok {
			return nil, errors.New("ReadSuperChunk: Could not parse address")
		}
		addr := []byte(a)
		s.AddChild(offset, metaType, addr)
	}
	return s, nil
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

func ReadDir(obj interface{}) (*Dir, error) {
	d := NewDir()
	data, ok := obj.([]interface{})
	if !ok {
		return nil, errors.New("ReadDir: Can't parse dir object to a list")
	}
	for _, e := range data {
		attributes := e.([]interface{})
		if len(attributes) != 5 {
			return nil, errors.New("ReadDir: Entry does not have 5 fields")
		}
		ft, ok := attributes[0].(int64)
		if !ok {
			return nil, errors.New("ReadDir: Could not parse file type")
		}
		fileType := byte(ft)
		m, ok := attributes[1].(int64)
		if !ok {
			return nil, errors.New("ReadDir: Could not parse mode")
		}
		mode := uint32(m)
		n, ok := attributes[2].(string)
		if !ok {
			return nil, errors.New("ReadDir: Could not parse name")
		}
		name := []byte(n)
		x, ok := attributes[3].(string)
		if !ok {
			return nil, errors.New("ReadDir: Could not parse xattr")
		}
		xattr := []byte(x)
		a, ok := attributes[4].(string)
		if !ok {
			return nil, errors.New("ReadDir: Could not parse address")
		}
		addr := []byte(a)
		d.AddEntry(fileType, mode, name, xattr, addr)
	}
	return d, nil
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

type Symlink struct {
	Target string
}

func (s *Symlink) toGoObj() interface{} {
	return []byte(s.Target)
}

func NewSymlink(target string) *Symlink {
	return &Symlink{Target: target}
}

func ReadSymlink(obj interface{}) (s *Symlink, err error) {
	target, ok := obj.(string)
	if !ok {
		return nil, errors.New("Could not read link")
	}
	return NewSymlink(target), nil
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
