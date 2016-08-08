package utils

import (
	"strings"
	"testing"
)

func TestSplitLastSimple(t *testing.T) {
	res := SplitLast("foo.bar.baz", ".")
	if strings.Compare(res, "baz") != 0 {
		t.Fatalf("expected 'baz', but result was '%s'", res)
	}
}
