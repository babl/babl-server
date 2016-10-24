package utils

import (
	"strconv"
	"strings"
)

func SplitFirst(s, sep string) string {
	n := strings.Index(s, sep)
	if n == -1 {
		return s
	}
	return s[:n]
}

func SplitLast(s, sep string) string {
	n := strings.LastIndex(s, sep)
	return s[n+1:]
}

func Check(err error) {
	if err != nil {
		panic(err)
	}
}

func FmtRid(rid uint64) string {
	return strconv.FormatUint(rid, 32)
}

func ParseRid(s string) (uint64, error) {
	return strconv.ParseUint(s, 32, 64)
}
