package opi

import (
	"bytes"
	"fmt"
	"log"
	"os/user"
	"time"

	"github.com/boltdb/bolt"
)

type DB struct {
	db         *bolt.DB
	bucketName []byte
}

func NewDB() Storage {
	// Determine where to store the db
	u, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	dbPath := u.HomeDir + "/.opi.db"
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	return &DB{db: db, bucketName: []byte("objects")}
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) Set(key, value []byte) error {
	go d.db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(d.bucketName)
		if err != nil {
			return err
		}
		err = bucket.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
	return nil
}

func (d *DB) Get(key []byte) (value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(d.bucketName)
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", d.bucketName)
		}
		var buffer bytes.Buffer
		buffer.Write(bucket.Get(key))
		value = buffer.Bytes()
		return nil
	})
	return
}

func (d *DB) Del(key []byte) (err error) { return nil }
func (d *DB) Hit(key []byte) (err error) { return nil }
