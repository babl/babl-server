package utils

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Testing with Ginkgo", func() {
	Context("SplitFirst", func() {
		It("split first simple", func() {
			Expect(SplitFirst("foo.bar.baz", ".")).To(Equal("foo"))
		})
		It("split first simple missing", func() {
			Expect(SplitFirst("foo bar baz", ".")).To(Equal("foo bar baz"))
		})
	})
	Context("SplitLast", func() {
		It("split last simple", func() {
			Expect(SplitLast("foo.bar.baz", ".")).To(Equal("baz"))
		})
		It("split last simple missing", func() {
			Expect(SplitLast("foo bar baz", ".")).To(Equal("foo bar baz"))
		})
	})
	Context("FmtRid", func() {
		It("convert to string", func() {
			Expect(FmtRid(10000)).To(Equal("9og"))
		})
	})
	Context("ParseRid", func() {
		It("convert from string", func() {
			n, err := ParseRid("9og")
			Expect(err).To(BeNil())
			Expect(n).To(Equal(uint64(10000)))
		})
		It("convert from string, large n", func() {
			n, err := ParseRid("bq94o6udtkamp")
			Expect(err).To(BeNil())
			Expect(n).To(Equal(uint64(13629185736935353049)))
		})
		It("converts a large n twice", func() {
			n := uint64(9223372036854775807)
			m, err := ParseRid(FmtRid(n))
			Expect(err).To(BeNil())
			Expect(n).To(Equal(m))
		})
	})
})
