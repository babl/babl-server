package utils

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Testing with Ginkgo", func() {
	It("split last simple", func() {
		Expect(SplitLast("foo.bar.baz", ".")).To(Equal("baz"))
	})
	It("split last simple missing", func() {
		Expect(SplitLast("foo bar baz", ".")).To(Equal("foo bar baz"))
	})
	It("split first simple", func() {
		Expect(SplitFirst("foo.bar.baz", ".")).To(Equal("foo"))
	})
	It("split first simple missing", func() {
		Expect(SplitFirst("foo bar baz", ".")).To(Equal("foo bar baz"))
	})
})
