// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"

	jujutesting "github.com/juju/testing"
	gc "gopkg.in/check.v1"

	"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
)

func TestPackage(t *testing.T) {
	jujutesting.MgoTestPackage(t, nil)
}

type BlobStoreSuite struct {
	jujutesting.IsolatedMgoSuite
}

var _ = gc.Suite(&BlobStoreSuite{})

func (s *BlobStoreSuite) TestPutTwice(c *gc.C) {
	store := blobstore.New(s.Session.DB("db"), "blobstore")

	content := "some data"
	err := store.PutUnchallenged(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	content = "some different data"
	err = store.PutUnchallenged(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	rc, length, err := store.Open("x")
	c.Assert(err, gc.IsNil)
	defer rc.Close()
	c.Assert(length, gc.Equals, int64(len(content)))

	data, err := ioutil.ReadAll(rc)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)
}

func (s *BlobStoreSuite) TestPutUnchallenged(c *gc.C) {
	store := blobstore.New(s.Session.DB("db"), "blobstore")

	content := "some data"
	err := store.PutUnchallenged(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	rc, length, err := store.Open("x")
	c.Assert(err, gc.IsNil)
	defer rc.Close()
	c.Assert(length, gc.Equals, int64(len(content)))

	data, err := ioutil.ReadAll(rc)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)

	err = store.PutUnchallenged(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)
}

func (s *BlobStoreSuite) TestPutUnchallengedInvalidHash(c *gc.C) {
	store := blobstore.New(s.Session.DB("db"), "blobstore")
	content := "some data"
	err := store.PutUnchallenged(strings.NewReader(content), "x", int64(len(content)), hashOf("wrong"))
	c.Assert(err, gc.ErrorMatches, "hash mismatch")
}

func (s *BlobStoreSuite) TestRemove(c *gc.C) {
	store := blobstore.New(s.Session.DB("db"), "blobstore")
	content := "some data"
	err := store.PutUnchallenged(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	rc, length, err := store.Open("x")
	c.Assert(err, gc.IsNil)
	defer rc.Close()
	c.Assert(length, gc.Equals, int64(len(content)))
	data, err := ioutil.ReadAll(rc)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)

	err = store.Remove("x")
	c.Assert(err, gc.IsNil)

	rc, length, err = store.Open("x")
	c.Assert(err, gc.ErrorMatches, `resource at path "[^"]+" not found`)
}

func hashOfReader(c *gc.C, r io.Reader) string {
	h := blobstore.NewHash()
	_, err := io.Copy(h, r)
	c.Assert(err, gc.IsNil)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func hashOf(s string) string {
	h := blobstore.NewHash()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

type dataSource struct {
	buf      []byte
	bufIndex int
	remain   int64
}

// newDataSource returns a stream of size bytes holding
// a repeated number.
func newDataSource(fillWith int64, size int64) io.Reader {
	src := &dataSource{
		remain: size,
	}
	for len(src.buf) < 8*1024 {
		src.buf = strconv.AppendInt(src.buf, fillWith, 10)
		src.buf = append(src.buf, ' ')
	}
	return src
}

func (s *dataSource) Read(buf []byte) (int, error) {
	if int64(len(buf)) > s.remain {
		buf = buf[:int(s.remain)]
	}
	total := len(buf)
	if total == 0 {
		return 0, io.EOF
	}

	for len(buf) > 0 {
		if s.bufIndex == len(s.buf) {
			s.bufIndex = 0
		}
		nb := copy(buf, s.buf[s.bufIndex:])
		s.bufIndex += nb
		buf = buf[nb:]
		s.remain -= int64(nb)
	}
	return total, nil
}
