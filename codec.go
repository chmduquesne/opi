package opi

import "github.com/golang/snappy"

type SimpleCodec struct{}

func (c *SimpleCodec) Encode(raw []byte) ([]byte, error) {
	return snappy.Encode(nil, raw), nil
}

func (c *SimpleCodec) Decode(enc []byte) ([]byte, error) {
	return snappy.Decode(nil, enc)
}

func NewSimpleCodec() Codec {
	return &SimpleCodec{}
}
