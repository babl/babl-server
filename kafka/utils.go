package kafka

import "strings"

func SplitGetByIndex(s, sep string, idx int) string {
	res := s
	if idx >= 0 && len(strings.Split(s, sep)) > idx {
		res = strings.Split(s, sep)[idx]
	}
	return res
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
