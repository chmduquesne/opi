package opi

import (
	"bytes"

	bencode "github.com/jackpal/bencode-go"
)

type SimpleCodec struct{}

func (c *SimpleCodec) Encode(obj interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := bencode.Marshal(&buf, obj)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *SimpleCodec) Decode(encoded []byte) (interface{}, error) {
	buf := bytes.NewReader(encoded)
	return bencode.Decode(buf)
}

func NewSimpleCodec() Codec {
	return &SimpleCodec{}
}
