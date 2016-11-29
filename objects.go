package opi

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	bencode "github.com/jackpal/bencode-go"
)

type FSObject interface {
	Bytes() ([]byte, error)
}

func bencoded(obj interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := bencode.Marshal(&buf, obj)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeError(field string, object string) error {
	msg := fmt.Sprintf("Could not decode field %s from object %s", field, object)
	return errors.New(msg)
}

// Chunk

type Chunk struct {
	Data []byte
}

func (c *Chunk) Bytes() ([]byte, error) {
	return c.Data, nil
}

func NewChunk(data []byte) *Chunk {
	return &Chunk{Data: data}
}

func ReadChunk(data []byte) (*Chunk, error) {
	return NewChunk(data), nil
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

func (s *SuperChunk) Bytes() ([]byte, error) {
	var obj [][3]interface{}
	for _, c := range s.Children {
		obj = append(obj, [3]interface{}{c.Offset, c.MetaType, c.Addr})
	}
	return bencoded(obj)
}

func ReadSuperChunk(data []byte) (*SuperChunk, error) {
	var obj [][3]interface{}
	r := bytes.NewReader(data)
	if err := bencode.Unmarshal(r, &obj); err != nil {
		return nil, err
	}
	s := NewSuperChunk()
	for _, c := range obj {
		offset, ok := c[0].(int64)
		if !ok {
			return nil, DecodeError("Chunk", "Offset")
		}
		metaType, ok := c[1].(int64)
		if !ok {
			return nil, DecodeError("Chunk", "MetaType")
		}
		addr, ok := c[2].(string)
		if !ok {
			return nil, DecodeError("Chunk", "Addr")
		}
		s.AddChild(
			uint64(offset),
			byte(metaType),
			[]byte(addr),
		)
	}
	return s, nil
}

func NewSuperChunk() *SuperChunk {
	return &SuperChunk{}
}

// Xattr

type Xattr struct {
	attributes map[string]string
}

// Dir

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

func (d *Dir) Bytes() ([]byte, error) {
	var obj [][5]interface{}
	for _, e := range d.Entries {
		obj = append(obj, [5]interface{}{
			e.FileType,
			e.Mode,
			e.Name,
			e.Xattr,
			e.Addr,
		})
	}
	return bencoded(obj)
}

func ReadDir(data []byte) (*Dir, error) {
	var obj [][5]interface{}
	r := bytes.NewReader(data)
	if err := bencode.Unmarshal(r, &obj); err != nil {
		return nil, err
	}
	d := NewDir()
	for _, e := range obj {
		fileType, ok := e[0].(int64)
		if !ok {
			return nil, DecodeError("FileType", "Dir")
		}
		mode, ok := e[1].(int64)
		if !ok {
			return nil, DecodeError("Mode", "Dir")
		}
		name, ok := e[2].(string)
		if !ok {
			return nil, DecodeError("Name", "Dir")
		}
		xattr, ok := e[3].(string)
		if !ok {
			return nil, DecodeError("Xattr", "Dir")
		}
		addr, ok := e[4].(string)
		if !ok {
			return nil, DecodeError("Addr", "Dir")
		}
		d.AddEntry(
			byte(fileType),
			uint32(mode),
			[]byte(name),
			[]byte(xattr),
			[]byte(addr),
		)
	}
	return d, nil
}

func NewDir() *Dir {
	return &Dir{}
}

// Commit

type Commit struct {
	Date    time.Time
	Tree    []byte
	Host    []byte
	Replica []byte
	Parents [][]byte
}

func (c *Commit) Bytes() ([]byte, error) {
	obj := [5]interface{}{
		c.Date.Format(time.UnixDate),
		c.Tree,
		c.Host,
		c.Replica,
		c.Parents,
	}
	return bencoded(obj)
}

func ReadCommit(data []byte) (*Commit, error) {
	var obj [5]interface{}
	r := bytes.NewReader(data)
	if err := bencode.Unmarshal(r, &obj); err != nil {
		return nil, err
	}
	fmtdate, ok := obj[0].(string)
	if !ok {
		return nil, DecodeError("Date", "Commit")
	}
	d, err := time.Parse(time.UnixDate, fmtdate)
	if err != nil {
		return nil, err
	}
	tree, ok := obj[1].(string)
	if !ok {
		return nil, DecodeError("Tree", "Commit")
	}
	host, ok := obj[2].(string)
	if !ok {
		return nil, DecodeError("Host", "Commit")
	}
	replica, ok := obj[3].(string)
	if !ok {
		return nil, DecodeError("Replica", "Commit")
	}
	parents := [][]byte{}
	p, ok := obj[4].([]string)
	if ok {
		for _, s := range p {
			parents = append(parents, []byte(s))
		}
	}
	c := NewCommit(
		d,
		[]byte(tree),
		[]byte(host),
		[]byte(replica),
		parents,
	)
	return c, nil
}

func (c *Commit) AddParent(addr []byte) {
	c.Parents = append(c.Parents, addr)
}

func NewCommit(date time.Time, tree []byte, host []byte, replica []byte, parents [][]byte) *Commit {
	d, _ := time.Parse(date.Format(time.UnixDate), time.UnixDate)
	return &Commit{
		Date:    d,
		Tree:    tree,
		Host:    host,
		Replica: replica,
		Parents: parents,
	}
}

// Symlink

type Symlink struct {
	Target string
}

func (s *Symlink) Bytes() ([]byte, error) {
	return []byte(s.Target), nil
}
func NewSymlink(target string) *Symlink {
	return &Symlink{Target: target}
}

func ReadSymlink(data []byte) (s *Symlink, err error) {
	target := string(data)
	return NewSymlink(target), nil
}
