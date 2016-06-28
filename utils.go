package main

import (
	"strings"
)

func SplitLast(s, sep string) string {
	n := strings.LastIndex(s, sep)
	return s[n+1:]
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
