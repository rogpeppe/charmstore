package entitycache_test

import (
	"reflect"
	"strings"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"

	"gopkg.in/juju/charmstore.v5-unstable/internal/entitycache"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

var _ = gc.Suite(&suite{})

type suite struct{}

type entityQuery struct {
	url    *charm.URL
	fields map[string]int
	reply  chan entityReply
}

type entityReply struct {
	entity *mongodoc.Entity
	err    error
}

type baseEntityQuery struct {
	url    *charm.URL
	fields map[string]int
	reply  chan baseEntityReply
}

type baseEntityReply struct {
	entity *mongodoc.BaseEntity
	err    error
}

func (*suite) TestEntityIssuesBaseEntityQuerySequentiallyForPromulgatedURL(c *gc.C) {
	c.Logf("x0")
	store := newChanStore()
	cache := entitycache.New(store)
	defer cache.Close()
	cache.AddBaseEntityFields(map[string]int{"name": 1})

	entity := &mongodoc.Entity{
		URL:            charm.MustParseURL("~bob/wordpress-1"),
		PromulgatedURL: charm.MustParseURL("wordpress-5"),
		BaseURL:        charm.MustParseURL("~bob/wordpress"),
		BlobName:       "w1",
		Size:           1,
	}
	baseEntity := &mongodoc.BaseEntity{
		URL:  charm.MustParseURL("~bob/wordpress"),
		Name: "wordpress",
	}
	c.Logf("x2")
	queryDone := make(chan struct{})
	go func() {
		defer close(queryDone)
		e, err := cache.Entity(charm.MustParseURL("wordpress-1"), map[string]int{"blobname": 1})
		c.Check(err, gc.IsNil)
		c.Check(e, jc.DeepEquals, selectEntityFields(entity, entityFields("blobname")))
	}()

	c.Logf("x3")
	// Acquire both the queries before replying so that we know they've been
	// issued concurrently.
	query1 := <-store.entityqc
	c.Assert(query1.url, jc.DeepEquals, charm.MustParseURL("wordpress-1"))
	c.Assert(query1.fields, jc.DeepEquals, entityFields("blobname"))
	query1.reply <- entityReply{
		entity: entity,
	}
	<-queryDone
	c.Logf("x4")

	// The base entity query is only issued when the original entity
	// is received. We can tell this because the URL in the query
	// contains the ~bob user which can't be inferred from the
	// original URL.
	query2 := <-store.baseEntityqc
	c.Assert(query2.url, jc.DeepEquals, charm.MustParseURL("~bob/wordpress"))
	c.Assert(query2.fields, jc.DeepEquals, baseEntityFields("name"))
	query2.reply <- baseEntityReply{
		entity: baseEntity,
	}

	c.Logf("x5")
	// Accessing the same entity again and the base entity should
	// not call any method on the store, so close the query channels
	// to ensure it doesn't.
	close(store.entityqc)
	close(store.baseEntityqc)
	c.Logf("x5.1")
	e, err := cache.Entity(charm.MustParseURL("wordpress-1"), map[string]int{"baseurl": 1, "blobname": 1})
	c.Check(err, gc.IsNil)
	c.Check(e, jc.DeepEquals, selectEntityFields(entity, entityFields("blobname")))
	c.Logf("x5.2")

	be, err := cache.BaseEntity(charm.MustParseURL("wordpress"), map[string]int{"name": 1})
	c.Check(err, gc.IsNil)
	c.Check(be, jc.DeepEquals, selectBaseEntityFields(baseEntity, baseEntityFields("name")))
	c.Logf("x6")
	c.Logf("test complete...")
}

// iterReply holds a reply from a request from a fakeIter
// for the next item.
type iterReply struct {
	// entity holds the entity to be replied with.
	// Any fields not specified when creating the
	// iterator will be omitted from the result
	// sent to the entitycache code.
	entity *mongodoc.Entity

	// err holds any iteration error. When the iteration is complete,
	// errIterFinished should be sent.
	err error
}

// fakeIter provides a mock iterator implementation
// that sends each request for an entity to
// another goroutine for a result.
type fakeIter struct {
	closed bool
	fields map[string]int
	err    error

	// req holds a channel that is sent a value
	// whenever the Next method is called.
	req chan chan iterReply
}

func newFakeIter() *fakeIter {
	return &fakeIter{
		req: make(chan chan iterReply, 1),
	}
}

func (i *fakeIter) SetFields(fields map[string]int) {
	i.fields = fields
}

// Next implements mgoIter.Next. The
// x parameter must be a *mongodoc.Entity.
func (i *fakeIter) Next(x interface{}) bool {
	if i.closed {
		panic("Next called after Close")
	}
	if i.err != nil {
		return false
	}
	replyc := make(chan iterReply)
	i.req <- replyc
	reply := <-replyc
	i.err = reply.err
	if i.err == nil {
		*(x.(*mongodoc.Entity)) = *selectEntityFields(reply.entity, i.fields)
	} else if reply.entity != nil {
		panic("entity with non-nil error")
	}

	return i.err == nil
}

var errIterFinished = errgo.New("iteration finished")

// Close implements mgoIter.Close.
func (i *fakeIter) Close() error {
	i.closed = true
	if i.err == errIterFinished {
		return nil
	}
	return i.err
}

// Close implements mgoIter.Err.
func (i *fakeIter) Err() error {
	if i.err == errIterFinished {
		return nil
	}
	return i.err
}

// sliceIter implements mgoIter over a slice of entities,
// returning each one in turn.
type sliceIter struct {
	fields   map[string]int
	entities []*mongodoc.Entity
	closed   bool
}

func (iter *sliceIter) SetFields(fields map[string]int) {
	iter.fields = fields
}

func (iter *sliceIter) Next(x interface{}) bool {
	if iter.closed {
		panic("Next called after Close")
	}
	if len(iter.entities) == 0 {
		return false
	}
	e := x.(*mongodoc.Entity)
	*e = *selectEntityFields(iter.entities[0], iter.fields)
	iter.entities = iter.entities[1:]
	return true
}

func (iter *sliceIter) Err() error {
	return nil
}

func (iter *sliceIter) Close() error {
	iter.closed = true
	return nil
}

type chanStore struct {
	entityqc     chan entityQuery
	baseEntityqc chan baseEntityQuery
	*callbackStore
}

func newChanStore() *chanStore {
	entityqc := make(chan entityQuery, 1)
	baseEntityqc := make(chan baseEntityQuery, 1)
	return &chanStore{
		entityqc:     entityqc,
		baseEntityqc: baseEntityqc,
		callbackStore: &callbackStore{
			findBestEntity: func(url *charm.URL, fields map[string]int) (*mongodoc.Entity, error) {
				reply := make(chan entityReply)
				entityqc <- entityQuery{
					url:    url,
					fields: fields,
					reply:  reply,
				}
				r := <-reply
				return r.entity, r.err
			},
			findBaseEntity: func(url *charm.URL, fields map[string]int) (*mongodoc.BaseEntity, error) {
				reply := make(chan baseEntityReply)
				baseEntityqc <- baseEntityQuery{
					url:    url,
					fields: fields,
					reply:  reply,
				}
				r := <-reply
				return r.entity, r.err
			},
		},
	}
}

type callbackStore struct {
	findBestEntity func(url *charm.URL, fields map[string]int) (*mongodoc.Entity, error)
	findBaseEntity func(url *charm.URL, fields map[string]int) (*mongodoc.BaseEntity, error)
}

func (s *callbackStore) FindBestEntity(url *charm.URL, fields map[string]int) (*mongodoc.Entity, error) {
	e, err := s.findBestEntity(url, fields)
	if err != nil {
		return nil, err
	}
	return selectEntityFields(e, fields), nil
}

func (s *callbackStore) FindBaseEntity(url *charm.URL, fields map[string]int) (*mongodoc.BaseEntity, error) {
	e, err := s.findBaseEntity(url, fields)
	if err != nil {
		return nil, err
	}
	return selectBaseEntityFields(e, fields), nil
}

type staticStore struct {
	entities     []*mongodoc.Entity
	baseEntities []*mongodoc.BaseEntity
}

func (s *staticStore) FindBestEntity(url *charm.URL, fields map[string]int) (*mongodoc.Entity, error) {
	for _, e := range s.entities {
		if *url == *e.URL {
			return selectEntityFields(e, fields), nil
		}
	}
	return nil, params.ErrNotFound
}

func (s *staticStore) FindBaseEntity(url *charm.URL, fields map[string]int) (*mongodoc.BaseEntity, error) {
	for _, e := range s.baseEntities {
		if *url == *e.URL {
			return e, nil
		}
	}
	return nil, params.ErrNotFound
}

func selectEntityFields(x *mongodoc.Entity, fields map[string]int) *mongodoc.Entity {
	e := selectFields(x, fields).(*mongodoc.Entity)
	if e.URL == nil {
		panic("url empty after selectfields")
	}
	return e
}

func selectBaseEntityFields(x *mongodoc.BaseEntity, fields map[string]int) *mongodoc.BaseEntity {
	return selectFields(x, fields).(*mongodoc.BaseEntity)
}

// selectFields returns a copy of x (which must
// be a pointer to struct) with all fields zeroed
// except those mentioned in fields.
func selectFields(x interface{}, fields map[string]int) interface{} {
	xv := reflect.ValueOf(x).Elem()
	xt := xv.Type()
	dv := reflect.New(xt).Elem()
	dv.Set(xv)
	for i := 0; i < xt.NumField(); i++ {
		f := xt.Field(i)
		if _, ok := fields[bsonFieldName(f)]; ok {
			continue
		}
		dv.Field(i).Set(reflect.Zero(f.Type))
	}
	return dv.Addr().Interface()
}

func bsonFieldName(f reflect.StructField) string {
	t := f.Tag.Get("bson")
	if t == "" {
		return strings.ToLower(f.Name)
	}
	if i := strings.Index(t, ","); i >= 0 {
		t = t[0:i]
	}
	if t != "" {
		return t
	}
	return strings.ToLower(f.Name)
}

func entityFields(fields ...string) map[string]int {
	return addFields(entitycache.RequiredEntityFields, fields...)
}

func baseEntityFields(fields ...string) map[string]int {
	return addFields(entitycache.RequiredBaseEntityFields, fields...)
}

func addFields(fields map[string]int, extra ...string) map[string]int {
	fields1 := make(map[string]int)
	for f := range fields {
		fields1[f] = 1
	}
	for _, f := range extra {
		fields1[f] = 1
	}
	return fields1
}
