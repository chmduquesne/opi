package opi

import (
	"bytes"
	"io/ioutil"
	"net/http"
)

type Client struct {
	host string
}

func NewClient() *Client {
	return &Client{host: Host()}
}

func (c *Client) Get(addr []byte) (value []byte, err error) {
	resp, err := http.Get(c.host + "/" + string(addr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	value, err = ioutil.ReadAll(resp.Body)
	return
}

func (c *Client) Set(addr, value []byte) (err error) {
	resp, err := http.Post(c.host+"/"+string(addr),
		"application/x-www-form-urlencoded",
		bytes.NewReader(value))
	defer resp.Body.Close()
	return err
}
