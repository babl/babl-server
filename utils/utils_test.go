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

func TestSplitLastSimpleMissing(t *testing.T) {
	res := SplitLast("foo bar baz", ".")
	if strings.Compare(res, "foo bar baz") != 0 {
		t.Fatalf("expected 'foo', but result was '%s'", res)
	}
}

func TestSplitFirstSimple(t *testing.T) {
	res := SplitFirst("foo.bar.baz", ".")
	if strings.Compare(res, "foo") != 0 {
		t.Fatalf("expected 'foo', but result was '%s'", res)
	}
}

func TestSplitFirstSimpleMissing(t *testing.T) {
	res := SplitFirst("foo bar baz", ".")
	if strings.Compare(res, "foo bar baz") != 0 {
		t.Fatalf("expected 'foo bar baz', but result was '%s'", res)
	}
}
