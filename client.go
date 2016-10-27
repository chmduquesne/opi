package opi

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Client struct {
	host string
}

func NewClient() Storage {
	return &Client{host: Host()}
}

func (c *Client) BaseURL() string {
	return "http://" + c.host + "/"
}

func (c *Client) Get(key []byte) (value []byte, err error) {
	resp, err := http.Get(c.BaseURL() + string(key))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	value, err = ioutil.ReadAll(resp.Body)
	return
}

func (c *Client) Set(key, value []byte) (err error) {
	resp, err := http.Post(c.BaseURL()+string(key),
		"application/x-www-form-urlencoded",
		bytes.NewReader(value))
	if err != nil {
		fmt.Errorf("%v", err)
	}
	defer resp.Body.Close()
	return err
}

func (c *Client) Del(key []byte) (err error) {
	return nil
}

func (c *Client) Hit(key []byte) (err error) {
	return nil
}
