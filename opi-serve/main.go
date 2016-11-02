package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"sync"
	"syscall"
	"time"

	"github.com/boltdb/bolt"
	"github.com/chmduquesne/opi"
)

type Storage struct {
	db         *bolt.DB
	bucketName []byte
	wg         sync.WaitGroup
}

func NewStorage() *Storage {
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
	return &Storage{db: db, bucketName: []byte("objects")}
}

func (s *Storage) Close() error {
	s.wg.Wait()
	return s.db.Close()
}

func (s *Storage) Set(key, value []byte) error {
	s.wg.Add(1)
	go s.db.Batch(func(tx *bolt.Tx) error {
		defer s.wg.Done()
		bucket, err := tx.CreateBucketIfNotExists(s.bucketName)
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

func (s *Storage) Get(key []byte) (value []byte, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucketName)
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", s.bucketName)
		}
		var buffer bytes.Buffer
		buffer.Write(bucket.Get(key))
		value = buffer.Bytes()
		return nil
	})
	return
}

func main() {
	s := NewStorage()
	defer s.Close()

	handler := func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[1:] // remove the '/' prefix
		//log.Printf("%v %v", r.Method, r.URL.Path)
		switch {
		case r.Method == "GET":
			value, err := s.Get([]byte(key))
			if err != nil || value == nil {
				w.WriteHeader(404)
			} else {
				w.Write(value)
			}
		case r.Method == "POST":
			value, err := ioutil.ReadAll(r.Body)
			err = s.Set([]byte(key), []byte(value))
			if err != nil {
				w.WriteHeader(500)
			}
		default:
			log.Printf("%s: method not supported", r.Method)
		}
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		s.Close()
		os.Exit(1)
	}()

	http.HandleFunc("/", handler)

	addr := opi.Host()
	log.Printf("Serving on http://%s\n", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
