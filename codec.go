package opi

import (
	"bytes"

	bencode "github.com/jackpal/bencode-go"
)

type SimpleCodec struct{}

func (c *SimpleCodec) Encode(obj interface{}) []byte {
	var buf bytes.Buffer
	bencode.Marshal(&buf, obj)
	return buf.Bytes()
}

func (c *SimpleCodec) Decode(encoded []byte) interface{} {
	buf := bytes.NewReader(encoded)
	var obj interface{}
	bencode.Unmarshal(buf, &obj)
	return obj
}

func NewSimpleCodec() Codec {
	return &SimpleCodec{}
}
