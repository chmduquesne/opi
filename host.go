package opi

import "os"

func Host() string {
	res := os.Getenv("OPI_SERVE_HOST")
	if res == "" {
		res = "127.0.0.1:30280"
	}
	return res
}
