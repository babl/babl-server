package utils

import "strings"

func SplitFirst(s, sep string) string {
	n := strings.Index(s, sep)
	if n == -1 {
		return ""
	}
	return s[:n]
}

func SplitLast(s, sep string) string {
	n := strings.LastIndex(s, sep)
	if n == -1 {
		return ""
	}
	return s[n+1:]
}

func Check(err error) {
	if err != nil {
		panic(err)
	}
}
