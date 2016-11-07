package opi

type Timeline interface {
	Archive(path string, name string) (err error)
	Restore(name string, path string) (err error)
}

type Storage interface {
	Get(key []byte) (value []byte, err error)
	Set(key []byte, value []byte) (err error)
	Del(key []byte) (err error)
	Hit(key []byte) (err error)
	Close() (err error)
}

type Codec interface {
	Encode(obj interface{}) (encoded []byte)
	Decode(encoded []byte) (obj interface{})
}
