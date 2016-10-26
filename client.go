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

func NewClient() *Client {
	return &Client{host: Host()}
}

func (c *Client) BaseURL() string {
	return "http://" + c.host + "/"
}

func (c *Client) Get(addr []byte) (value []byte, err error) {
	resp, err := http.Get(c.BaseURL() + string(addr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	value, err = ioutil.ReadAll(resp.Body)
	return
}

func (c *Client) Set(addr, value []byte) (err error) {
	resp, err := http.Post(c.BaseURL()+string(addr),
		"application/x-www-form-urlencoded",
		bytes.NewReader(value))
	if err != nil {
		fmt.Errorf("%v", err)
	}
	defer resp.Body.Close()
	return err
}
