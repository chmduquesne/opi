package opi

import (
	"bytes"
	"io/ioutil"
	"net/http"
)

type Client struct {
	http.Client
	Host string
}

func NewClient() Storage {
	return &Client{
		Host: "http://" + Host() + "/",
	}
}

func (c *Client) Get(key []byte) (value []byte, err error) {
	resp, err := c.Client.Get(c.Host + string(key))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	value, err = ioutil.ReadAll(resp.Body)
	return
}

func (c *Client) Set(key, value []byte) (err error) {
	resp, err := c.Client.Post(c.Host+string(key),
		"application/x-www-form-urlencoded",
		bytes.NewReader(value))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return
}

func (c *Client) Del(key []byte) (err error) {
	return nil
}

func (c *Client) Hit(key []byte) (err error) {
	return nil
}

func (c *Client) Close() (err error) {
	return nil
}
