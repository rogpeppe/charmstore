// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"net/http"
	"sort"
	"sync"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v4"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/juju/charmstore/internal/mongodoc"
	"github.com/juju/charmstore/internal/storetesting"
	"github.com/juju/charmstore/params"
)

type migrationsSuite struct {
	storetesting.IsolatedMgoSuite
	db       StoreDatabase
	executed []string
}

var _ = gc.Suite(&migrationsSuite{})

func (s *migrationsSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	s.db = StoreDatabase{s.Session.DB("migration-testing")}
	s.executed = []string{}
}

func (s *migrationsSuite) newServer(c *gc.C) error {
	apiHandler := func(store *Store, config ServerParams) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
	}
	_, err := NewServer(s.db.Database, nil, serverParams, map[string]NewAPIHandlerFunc{
		"version1": apiHandler,
	})
	return err
}

// patchMigrations patches the charm store migration list with the given ms.
func (s *migrationsSuite) patchMigrations(c *gc.C, ms []migration) {
	original := migrations
	s.AddCleanup(func(*gc.C) {
		migrations = original
	})
	migrations = ms
}

// makeMigrations generates default migrations using the given names, and then
// patches the charm store migration list with the generated ones.
func (s *migrationsSuite) makeMigrations(c *gc.C, names ...string) {
	ms := make([]migration, len(names))
	for i, name := range names {
		name := name
		ms[i] = migration{
			name: name,
			migrate: func(StoreDatabase) error {
				s.executed = append(s.executed, name)
				return nil
			},
		}
	}
	s.patchMigrations(c, ms)
}

func (s *migrationsSuite) TestMigrate(c *gc.C) {
	// Create migrations.
	names := []string{"migr-1", "migr-2"}
	s.makeMigrations(c, names...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// The two migrations have been correctly executed in order.
	c.Assert(s.executed, jc.DeepEquals, names)

	// The migration document in the db reports that the execution is done.
	s.checkExecuted(c, names...)

	// Restart the server again and check migrations this time are not run.
	err = s.newServer(c)
	c.Assert(err, gc.IsNil)
	c.Assert(s.executed, jc.DeepEquals, names)
	s.checkExecuted(c, names...)
}

func (s *migrationsSuite) TestMigrateNoMigrations(c *gc.C) {
	// Empty the list of migrations.
	s.makeMigrations(c)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// No migrations were executed.
	c.Assert(s.executed, gc.HasLen, 0)
	s.checkExecuted(c)
}

func (s *migrationsSuite) TestMigrateNewMigration(c *gc.C) {
	// Simulate two migrations were already run.
	err := setExecuted(s.db, "migr-1")
	c.Assert(err, gc.IsNil)
	err = setExecuted(s.db, "migr-2")
	c.Assert(err, gc.IsNil)

	// Create migrations.
	s.makeMigrations(c, "migr-1", "migr-2", "migr-3")

	// Start the server.
	err = s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Only one migration has been executed.
	c.Assert(s.executed, jc.DeepEquals, []string{"migr-3"})

	// The migration document in the db reports that the execution is done.
	s.checkExecuted(c, "migr-1", "migr-2", "migr-3")
}

func (s *migrationsSuite) TestMigrateErrorUnknownMigration(c *gc.C) {
	// Simulate that a migration was already run.
	err := setExecuted(s.db, "migr-1")
	c.Assert(err, gc.IsNil)

	// Create migrations, without including the already executed one.
	s.makeMigrations(c, "migr-2", "migr-3")

	// Start the server.
	err = s.newServer(c)
	c.Assert(err, gc.ErrorMatches, `database migration failed: found unknown migration "migr-1"; running old charm store code on newer charm store database\?`)

	// No new migrations were executed.
	c.Assert(s.executed, gc.HasLen, 0)
	s.checkExecuted(c, "migr-1")
}

func (s *migrationsSuite) TestMigrateErrorExecutingMigration(c *gc.C) {
	ms := []migration{{
		name: "migr-1",
		migrate: func(StoreDatabase) error {
			return nil
		},
	}, {
		name: "migr-2",
		migrate: func(StoreDatabase) error {
			return errgo.New("bad wolf")
		},
	}, {
		name: "migr-3",
		migrate: func(StoreDatabase) error {
			return nil
		},
	}}
	s.patchMigrations(c, ms)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.ErrorMatches, "database migration failed: error executing migration: migr-2: bad wolf")

	// Only one migration has been executed.
	s.checkExecuted(c, "migr-1")
}

func (s *migrationsSuite) TestMigrateMigrationNames(c *gc.C) {
	names := make(map[string]bool, len(migrations))
	for _, m := range migrations {
		c.Assert(names[m.name], jc.IsFalse, gc.Commentf("multiple migrations named %q", m.name))
		names[m.name] = true
	}
}

func (s *migrationsSuite) TestMigrateMigrationList(c *gc.C) {
	// When adding migration, update the list below, but never remove existing
	// migrations.
	existing := []string{
		"entity ids denormalization",
		"base entities creation",
		"read acl creation",
		"write acl creation",
		"populate promulgated entities",
	}
	for i, name := range existing {
		m := migrations[i]
		c.Assert(m.name, gc.Equals, name)
	}
}

func (s *migrationsSuite) TestMigrateParallelMigration(c *gc.C) {
	// This test uses real migrations to check they are idempotent and works
	// well when done in parallel, for example when multiple charm store units
	// are deployed together.

	// Prepare a database for the denormalizeEntityIds migration.
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("~who/utopic/rails-47")
	id3 := charm.MustParseReference("~charmers/trusty/django-18")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id1,
		BlobHash: "hash1",
		Size:     12,
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"charmers"`),
		},
	}), entity1fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id2,
		BlobHash: "hash2",
		Size:     13,
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity1fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id3,
		BaseURL:  baseURL(id3),
		BlobHash: "hash1",
		Size:     12,
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"charmers"`),
		},
	}), entity1fields...)

	// Run the migrations in parallel.
	var wg sync.WaitGroup
	wg.Add(5)
	errors := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func() {
			errors <- s.newServer(c)
			wg.Done()
		}()
	}
	wg.Wait()
	close(errors)

	// Check the server is correctly started in all the units.
	for err := range errors {
		c.Assert(err, gc.IsNil)
	}

	// Ensure entities have been updated correctly by denormalizeEntityIds.
	s.checkCount(c, s.db.Entities(), 2)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id3,
		BlobHash: "hash1",
		Size:     12,
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"charmers"`),
		},
		PromulgatedURL: id1,
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id2,
		BlobHash: "hash2",
		Size:     13,
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)
}

func (s *migrationsSuite) checkExecuted(c *gc.C, expected ...string) {
	var obtained []string
	var doc mongodoc.Migration
	if err := s.db.Migrations().Find(nil).One(&doc); err != mgo.ErrNotFound {
		c.Assert(err, gc.IsNil)
		obtained = doc.Executed
		sort.Strings(obtained)
	}
	sort.Strings(expected)
	c.Assert(obtained, jc.DeepEquals, expected)
}

func getMigrations(names ...string) (ms []migration) {
	for _, name := range names {
		for _, m := range migrations {
			if m.name == name {
				ms = append(ms, m)
			}
		}
	}
	return ms
}

func (s *migrationsSuite) TestDenormalizeEntityIds(c *gc.C) {
	s.patchMigrations(c, getMigrations("entity ids denormalization"))
	// Store entities with missing name in the db.
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("~who/utopic/rails-47")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 12}), entity1fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 13}), entity1fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure entities have been updated correctly.
	s.checkCount(c, s.db.Entities(), 2)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:  id1,
		Size: 12,
	}), entity2fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:  id2,
		Size: 13,
	}), entity2fields...)
}

func (s *migrationsSuite) TestDenormalizeEntityIdsNoEntities(c *gc.C) {
	s.patchMigrations(c, getMigrations("entity ids denormalization"))
	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure no new entities are added in the process.
	s.checkCount(c, s.db.Entities(), 0)
}

func (s *migrationsSuite) TestDenormalizeEntityIdsNoUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("entity ids denormalization"))
	// Store entities with a name in the db.
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("~who/utopic/rails-47")
	fields := append(entity2fields, "user", "series", "revision")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 21}), fields...)
	s.insertEntity(c, &mongodoc.Entity{
		URL:     id2,
		BaseURL: baseURL(id2),
		Name:    "rails2",
		Size:    22,
	}, fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure entities have been updated correctly.
	s.checkCount(c, s.db.Entities(), 2)
	s.checkEntity(c, &mongodoc.Entity{
		URL:     id1,
		BaseURL: baseURL(id1),
		// Since the name field already existed, the Revision and Series fields
		// have not been populated.
		Name: "django",
		Size: 21,
	}, entity2fields...)
	s.checkEntity(c, &mongodoc.Entity{
		URL:     id2,
		BaseURL: baseURL(id2),
		// The name is left untouched (even if it's obviously wrong).
		Name: "rails2",
		// Since the name field already existed, the User, Revision and Series
		// fields have not been populated.
		Size: 22,
	}, entity2fields...)
}

func (s *migrationsSuite) TestDenormalizeEntityIdsSomeUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("entity ids denormalization"))
	// Store entities with and without names in the db
	id1 := charm.MustParseReference("~dalek/utopic/django-42")
	id2 := charm.MustParseReference("~dalek/utopic/django-47")
	id3 := charm.MustParseReference("precise/postgres-0")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 1}), entity1fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 2}), append(entity2fields, "user", "revision", "series")...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id3, Size: 3}), entity1fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure entities have been updated correctly.
	s.checkCount(c, s.db.Entities(), 3)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:  id1,
		Size: 1,
	}), entity2fields...)
	s.checkEntity(c, &mongodoc.Entity{
		URL:      id2,
		BaseURL:  baseURL(id2),
		Name:     "django",
		User:     "",
		Revision: 0,
		Series:   "",
		Size:     2,
	}, entity2fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:  id3,
		Size: 3,
	}), entity2fields...)
}

func (s *migrationsSuite) TestCreateBaseEntities(c *gc.C) {
	s.patchMigrations(c, getMigrations("base entities creation"))
	// Store entities with missing base in the db.
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("trusty/django-47")
	id3 := charm.MustParseReference("~who/utopic/rails-47")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 12}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 12}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id3, Size: 13}), entity2fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure base entities have been created correctly.
	s.checkCount(c, s.db.BaseEntities(), 2)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id1),
	}), baseEntity1fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id3),
	}), baseEntity1fields...)
}

func (s *migrationsSuite) TestCreateBaseEntitiesNoEntities(c *gc.C) {
	s.patchMigrations(c, getMigrations("base entities creation"))
	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure no new base entities are added in the process.
	s.checkCount(c, s.db.BaseEntities(), 0)
}

func (s *migrationsSuite) TestCreateBaseEntitiesNoUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("base entities creation"))
	// Store entities with their corresponding base in the db.
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("~who/utopic/rails-47")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 21}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 22}), entity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id1)}), baseEntity1fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id2)}), baseEntity1fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure no new base entities are added in the process.
	s.checkCount(c, s.db.BaseEntities(), 2)
}

func (s *migrationsSuite) TestCreateBaseEntitiesSomeUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("base entities creation"))
	// Store entities with and without bases in the db
	id1 := charm.MustParseReference("~dalek/utopic/django-42")
	id2 := charm.MustParseReference("~dalek/utopic/django-47")
	id3 := charm.MustParseReference("precise/postgres-0")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 1}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 2}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id3, Size: 3}), entity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id2)}), baseEntity1fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure missing base entities have been created correctly.
	s.checkCount(c, s.db.BaseEntities(), 2)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id1),
	}), baseEntity1fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id3),
	}), baseEntity1fields...)
}

func (s *migrationsSuite) TestPopulateReadACL(c *gc.C) {
	s.patchMigrations(c, getMigrations("read acl creation"))
	// Store entities with their base in the db.
	// The base entities will not include any read permission.
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("trusty/django-47")
	id3 := charm.MustParseReference("~who/utopic/rails-47")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 12}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 12}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id3, Size: 13}), entity2fields...)
	baseId1 := baseURL(id1)
	baseId3 := baseURL(id3)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseId1}), baseEntity1fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseId3}), baseEntity1fields...)

	// Ensure read permission is empty.
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId1,
	}), baseEntity1fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId3,
	}), baseEntity1fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure read permission has been correctly set.
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId1,
		ACLs: mongodoc.ACL{
			Read: []string{params.Everyone},
		},
	}), baseEntity2fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId3,
		ACLs: mongodoc.ACL{
			Read: []string{params.Everyone, "who"},
		},
	}), baseEntity2fields...)
}

func (s *migrationsSuite) TestCreateBaseEntitiesAndPopulateReadACL(c *gc.C) {
	s.patchMigrations(c, getMigrations("base entities creation", "read acl creation"))
	// Store entities with missing base in the db.
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("trusty/django-47")
	id3 := charm.MustParseReference("~who/utopic/rails-47")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 12}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 12}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id3, Size: 13}), entity2fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure base entities have been created correctly.
	s.checkCount(c, s.db.BaseEntities(), 2)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id1),
		ACLs: mongodoc.ACL{
			Read: []string{params.Everyone},
		},
	}), baseEntity2fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id3),
		ACLs: mongodoc.ACL{
			Read: []string{params.Everyone, "who"},
		},
	}), baseEntity2fields...)
}

func (s *migrationsSuite) TestPopulateReadACLNoEntities(c *gc.C) {
	s.patchMigrations(c, getMigrations("read acl creation"))
	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure no new base entities are added in the process.
	s.checkCount(c, s.db.BaseEntities(), 0)
}

func (s *migrationsSuite) TestPopulateReadACLNoUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("read acl creation"))
	// Store entities with their corresponding base in the db.
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("~who/utopic/rails-47")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 21}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 22}), entity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id1),
		ACLs: mongodoc.ACL{
			Read: []string{"jean-luc"},
		},
	}), baseEntity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id2),
		ACLs: mongodoc.ACL{
			Read: []string{"who"},
		},
	}), baseEntity2fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure no new base entities are added in the process, and read
	// permissions were not changed.
	s.checkCount(c, s.db.BaseEntities(), 2)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id1),
		ACLs: mongodoc.ACL{
			Read: []string{"jean-luc"},
		},
	}), baseEntity2fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id2),
		ACLs: mongodoc.ACL{
			Read: []string{"who"},
		},
	}), baseEntity2fields...)
}

func (s *migrationsSuite) TestPopulateReadACLSomeUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("read acl creation"))
	// Store entities with and without bases in the db
	id1 := charm.MustParseReference("~dalek/utopic/django-42")
	id2 := charm.MustParseReference("~dalek/utopic/django-47")
	id3 := charm.MustParseReference("precise/postgres-0")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 1}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 2}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id3, Size: 3}), entity2fields...)
	baseId1 := baseURL(id1)
	baseId3 := baseURL(id3)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseId1}), baseEntity1fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId3,
		ACLs: mongodoc.ACL{
			Read: []string{"benjamin"},
		},
	}), baseEntity2fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure missing read permissions have been populated correctly.
	s.checkCount(c, s.db.BaseEntities(), 2)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId1,
		ACLs: mongodoc.ACL{
			Read: []string{params.Everyone, "dalek"},
		},
	}), baseEntity2fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId3,
		ACLs: mongodoc.ACL{
			Read: []string{"benjamin"},
		},
	}), baseEntity2fields...)
}

func (s *migrationsSuite) TestPopulateWriteACL(c *gc.C) {
	s.patchMigrations(c, getMigrations("write acl creation"))
	// Store entities with their base in the db.
	// The base entities will not include any write permission.
	id1 := charm.MustParseReference("~who/trusty/django-42")
	id2 := charm.MustParseReference("~who/django-47")
	id3 := charm.MustParseReference("~dalek/utopic/rails-47")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 12}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 12}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id3, Size: 13}), entity2fields...)
	baseId1 := baseURL(id1)
	baseId3 := baseURL(id3)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseId1}), baseEntity1fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseId3}), baseEntity1fields...)

	// Ensure write permission is empty.
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId1,
	}), baseEntity1fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId3,
	}), baseEntity1fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure write permission has been correctly set.
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId1,
		ACLs: mongodoc.ACL{
			Write: []string{"who"},
		},
	}), baseEntity2fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId3,
		ACLs: mongodoc.ACL{
			Write: []string{"dalek"},
		},
	}), baseEntity2fields...)
}

func (s *migrationsSuite) TestPopulateWriteACLNoEntities(c *gc.C) {
	s.patchMigrations(c, getMigrations("write acl creation"))
	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure no new base entities are added in the process.
	s.checkCount(c, s.db.BaseEntities(), 0)
}

func (s *migrationsSuite) TestPopulateWriteACLNoUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("write acl creation"))
	// Store entities with their corresponding base in the db.
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("~who/utopic/rails-47")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 21}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 22}), entity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id1)}), baseEntity1fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id2),
		ACLs: mongodoc.ACL{
			Write: []string{"dalek"},
		},
	}), baseEntity2fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure no new base entities are added in the process, and write
	// permissions were not changed.
	s.checkCount(c, s.db.BaseEntities(), 2)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id1),
	}), baseEntity2fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id2),
		ACLs: mongodoc.ACL{
			Write: []string{"dalek"},
		},
	}), baseEntity2fields...)
}

func (s *migrationsSuite) TestPopulateWriteACLSomeUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("write acl creation"))
	// Store entities with and without bases in the db
	id1 := charm.MustParseReference("~dalek/utopic/django-42")
	id2 := charm.MustParseReference("~dalek/utopic/django-47")
	id3 := charm.MustParseReference("~jean-luc/precise/postgres-0")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id1, Size: 1}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id2, Size: 2}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{URL: id3, Size: 3}), entity2fields...)
	baseId1 := baseURL(id1)
	baseId3 := baseURL(id3)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseId1}), baseEntity1fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId3,
		ACLs: mongodoc.ACL{
			Write: []string{"benjamin"},
		},
	}), baseEntity2fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure missing write permissions have been populated correctly.
	s.checkCount(c, s.db.BaseEntities(), 2)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId1,
		ACLs: mongodoc.ACL{
			Write: []string{"dalek"},
		},
	}), baseEntity2fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseId3,
		ACLs: mongodoc.ACL{
			Write: []string{"benjamin"},
		},
	}), baseEntity2fields...)
}

func (s *migrationsSuite) TestPopulatePromulgatedEntities(c *gc.C) {
	s.patchMigrations(c, getMigrations("populate promulgated entities"))
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("~who/utopic/rails-47")
	id3 := charm.MustParseReference("~who/trusty/django-42")
	id4 := charm.MustParseReference("~dalek/trusty/django-42")
	id5 := charm.MustParseReference("~ace/utopic/rails-47")
	id6 := charm.MustParseReference("~who/trusty/django-41")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id1,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id2,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id3,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id4,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"dalek"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id5,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"ace"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id6,
		BlobHash: "django-2",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity2fields...)

	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id1)}), baseEntity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id2)}), baseEntity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id3)}), baseEntity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id4)}), baseEntity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id5)}), baseEntity2fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure entities have been updated correctly.
	s.checkCount(c, s.db.Entities(), 5)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id2,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:            id3,
		BlobHash:       "django-1",
		PromulgatedURL: id1,
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id4,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"dalek"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id5,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"ace"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id6,
		BlobHash: "django-2",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)

	s.checkCount(c, s.db.BaseEntities(), 4)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id2),
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL:         baseURL(id3),
		Promulgated: true,
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id4),
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id5),
	}), baseEntity3fields...)
}

func (s *migrationsSuite) TestPopulatePromulgatedEntitiesNoEntities(c *gc.C) {
	s.patchMigrations(c, getMigrations("populate promulgated entities"))
	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure no new entities are added in the process.
	s.checkCount(c, s.db.Entities(), 0)
}

func (s *migrationsSuite) TestPopulatePromulgatedEntitiesNoUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("populate promulgated entities"))
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("~who/utopic/rails-47")
	id3 := charm.MustParseReference("~who/trusty/django-42")
	id4 := charm.MustParseReference("~dalek/trusty/django-42")
	id5 := charm.MustParseReference("~ace/utopic/rails-47")
	id6 := charm.MustParseReference("~who/trusty/django-41")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id2,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id3,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
		PromulgatedURL:      id1,
		PromulgatedRevision: id1.Revision,
	}), entity3fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id4,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"dalek"`),
		},
	}), entity3fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id5,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"ace"`),
		},
	}), entity3fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id6,
		BlobHash: "django-2",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)

	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id2)}), baseEntity3fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL:         baseURL(id3),
		Promulgated: true,
	}), baseEntity3fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id4)}), baseEntity3fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id5)}), baseEntity3fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure entities have not been changed.
	s.checkCount(c, s.db.Entities(), 5)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id2,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:            id3,
		BlobHash:       "django-1",
		PromulgatedURL: id1,
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id4,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"dalek"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id5,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"ace"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id6,
		BlobHash: "django-2",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)

	s.checkCount(c, s.db.BaseEntities(), 4)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id2),
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL:         baseURL(id3),
		Promulgated: true,
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id4),
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id5),
	}), baseEntity3fields...)
}

func (s *migrationsSuite) TestPopulatePromulgatedEntitiesSomeUpdates(c *gc.C) {
	s.patchMigrations(c, getMigrations("populate promulgated entities"))
	id1 := charm.MustParseReference("trusty/django-42")
	id2 := charm.MustParseReference("~who/utopic/rails-47")
	id3 := charm.MustParseReference("~who/trusty/django-42")
	id4 := charm.MustParseReference("~dalek/trusty/django-42")
	id5 := charm.MustParseReference("~ace/utopic/rails-47")
	id6 := charm.MustParseReference("~who/trusty/django-41")
	id7 := charm.MustParseReference("trusty/django-41")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id1,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id2,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id3,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id4,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"dalek"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id5,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"ace"`),
		},
	}), entity2fields...)
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id6,
		BlobHash: "django-2",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
		PromulgatedURL: id7,
	}), entity3fields...)

	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id1)}), baseEntity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id2)}), baseEntity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL:         baseURL(id3),
		Promulgated: true,
	}), baseEntity3fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id4)}), baseEntity2fields...)
	s.insertBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{URL: baseURL(id5)}), baseEntity2fields...)

	// Start the server.
	err := s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Ensure entities have been updated correctly.
	s.checkCount(c, s.db.Entities(), 5)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id2,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:            id3,
		BlobHash:       "django-1",
		PromulgatedURL: id1,
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id4,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"dalek"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id5,
		BlobHash: "rails-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"ace"`),
		},
	}), entity3fields...)
	s.checkEntity(c, fillEntity(&mongodoc.Entity{
		URL:            id6,
		BlobHash:       "django-2",
		PromulgatedURL: id7,
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`"who"`),
		},
	}), entity3fields...)

	s.checkCount(c, s.db.BaseEntities(), 4)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id2),
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL:         baseURL(id3),
		Promulgated: true,
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id4),
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: baseURL(id5),
	}), baseEntity3fields...)
}

func (s *migrationsSuite) TestPopulatePromulgatedEntitiesNoBzrOwner(c *gc.C) {
	s.patchMigrations(c, getMigrations("populate promulgated entities"))
	id1 := charm.MustParseReference("trusty/django-42")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:       id1,
		BlobHash:  "django-1",
		ExtraInfo: map[string][]byte{},
	}), entity2fields...)

	err := s.newServer(c)
	c.Assert(err, gc.ErrorMatches, "database migration failed: error executing migration: populate promulgated entities: cannot unmarshal user from extra-info: unexpected end of JSON input")
}

func (s *migrationsSuite) TestPopulatePromulgatedEntitiesBlankBzrOwner(c *gc.C) {
	s.patchMigrations(c, getMigrations("populate promulgated entities"))
	id1 := charm.MustParseReference("trusty/django-42")
	s.insertEntity(c, fillEntity(&mongodoc.Entity{
		URL:      id1,
		BlobHash: "django-1",
		ExtraInfo: map[string][]byte{
			"bzr-owner": []byte(`""`),
		},
	}), entity2fields...)

	err := s.newServer(c)
	c.Assert(err, gc.ErrorMatches, `database migration failed: error executing migration: populate promulgated entities: no user for "cs:trusty/django-42"`)
}

func (s *migrationsSuite) TestPopulatePromulgatedEntitiesUpdatesBaseEntities(c *gc.C) {
	s.patchMigrations(c, getMigrations("populate promulgated entities"))

	id1 := charm.MustParseReference("~tom/wordpress")
	id2 := charm.MustParseReference("~dick/wordpress")
	id3 := charm.MustParseReference("~harry/wordpress")
	err := s.db.BaseEntities().Insert(bson.D{
		{"_id", id1},
		{"user", id1.User},
		{"name", id1.Name},
		{"public", true},
		{"promulgated", true},
	})
	c.Assert(err, gc.IsNil)
	err = s.db.BaseEntities().Insert(bson.D{
		{"_id", id2},
		{"user", id2.User},
		{"name", id2.Name},
		{"public", true},
		{"promulgated", false},
	})
	c.Assert(err, gc.IsNil)
	err = s.db.BaseEntities().Insert(bson.D{
		{"_id", id3},
		{"user", id3.User},
		{"name", id3.Name},
		{"public", true},
	})
	c.Assert(err, gc.IsNil)

	err = s.newServer(c)
	c.Assert(err, gc.IsNil)

	// Check the updated base entities
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: id1,
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: id2,
	}), baseEntity3fields...)
	s.checkBaseEntity(c, fillBaseEntity(&mongodoc.BaseEntity{
		URL: id3,
	}), baseEntity3fields...)
}

func (s *migrationsSuite) checkEntity(c *gc.C, expectEntity *mongodoc.Entity, fields ...string) {
	var expect, obtained mongodoc.Entity
	expect = *expectEntity
	// zero any fields in expect that are not expected to be present.
	for _, f := range fields {
		if f == "promulgated-revision" {
			expect.PromulgatedRevision = 0
		}
		if f == "promulgated-url" {
			expect.PromulgatedURL = nil
		}
		if f == "name" {
			expect.Name = ""
		}
		if f == "revision" {
			expect.Revision = 0
		}
		if f == "series" {
			expect.Series = ""
		}
		if f == "user" {
			expect.User = ""
		}
	}
	err := s.db.Entities().FindId(expectEntity.URL).One(&obtained)
	c.Assert(err, gc.IsNil)

	// Ensure that the denormalized fields are now present, and the previously
	// existing fields are still there.
	c.Assert(obtained, jc.DeepEquals, expect)
}

func (s *migrationsSuite) checkCount(c *gc.C, coll *mgo.Collection, expectCount int) {
	count, err := coll.Count()
	c.Assert(err, gc.IsNil)
	c.Assert(count, gc.Equals, expectCount)
}

func (s *migrationsSuite) checkBaseEntity(c *gc.C, expectEntity *mongodoc.BaseEntity, fields ...string) {
	var expect, obtained mongodoc.BaseEntity
	expect = *expectEntity
	// zero any fields in expect that are not expected to be present.
	for _, f := range fields {
		if f == "promulgated" {
			expect.Promulgated = false
		}
		if f == "public" {
			expect.Public = false
		}
		if f == "acls" {
			expect.ACLs = mongodoc.ACL{}
		}
	}
	err := s.db.BaseEntities().FindId(expectEntity.URL).One(&obtained)
	c.Assert(err, gc.IsNil)
	c.Assert(obtained, jc.DeepEquals, expect)
}

// fillEntity updates the provided mongodoc.Entity filling in
// the following fields with values derived from URL and
// PromulgatedURL:
//
//     BaseURL
//     Name
//     Revision
//     Series
//     User
//     PromulgatedRevision
//
// The provided entity is returned by this function.
func fillEntity(e *mongodoc.Entity) *mongodoc.Entity {
	e.BaseURL = baseURL(e.URL)
	e.Name = e.URL.Name
	e.Revision = e.URL.Revision
	e.Series = e.URL.Series
	e.User = e.URL.User
	if e.PromulgatedURL != nil {
		e.PromulgatedRevision = e.PromulgatedURL.Revision
	} else {
		e.PromulgatedRevision = -1
	}
	return e
}

// insertEntity creates a new entity in the database based on the
// provided mongodoc.Entity.
//
// After insertion any name in fields will be removed from the inserted
// document.
func (s *migrationsSuite) insertEntity(c *gc.C, entity *mongodoc.Entity, fields ...string) {
	err := s.db.Entities().Insert(entity)
	c.Assert(err, gc.IsNil)

	// Remove required fields
	var remove bson.D
	for _, f := range fields {
		remove = append(remove, bson.DocElem{f, true})
	}
	if len(remove) == 0 {
		return
	}
	err = s.db.Entities().UpdateId(entity.URL, bson.D{{"$unset", remove}})
	c.Assert(err, gc.IsNil)
}

// Standard entity versions. These are lists of fields that can be used with
// insertEntity to create database documents from previous deployments.
var (
	entity3fields = []string{}
	entity2fields = append([]string{"promulgated-url", "promulgated-revision"}, entity3fields...)
	entity1fields = append([]string{"name", "revision", "series", "user"}, entity2fields...)
)

// fillBaseEntity updates the provided mongodoc.BaseEntity filling in
// the following fields with values derived from URL:
//
//     Name
//     User
//     Public (always set true)
//
// The provided base entity is returned by this function.
func fillBaseEntity(e *mongodoc.BaseEntity) *mongodoc.BaseEntity {
	e.Name = e.URL.Name
	e.User = e.URL.User
	e.Public = true
	return e
}

// insertBaseEntity creates a new base entity in the database based on the
// provided mongodoc.BaseEntity.
//
// After insertion any name in fields will be removed from the inserted
// document.
func (s *migrationsSuite) insertBaseEntity(c *gc.C, baseEntity *mongodoc.BaseEntity, fields ...string) {
	err := s.db.BaseEntities().Insert(baseEntity)
	c.Assert(err, gc.IsNil)

	// Remove required fields
	var remove bson.D
	for _, f := range fields {
		remove = append(remove, bson.DocElem{f, true})
	}
	if len(remove) == 0 {
		return
	}
	err = s.db.BaseEntities().UpdateId(baseEntity.URL, bson.D{{"$unset", remove}})
	c.Assert(err, gc.IsNil)
}

// Standard base entity versions. These are lists of fields that can be
// used with insertBaseEntity to create database documents from previous
// deployments.
var (
	baseEntity3fields = []string{}
	baseEntity2fields = append([]string{"promulgated"}, baseEntity3fields...)
	baseEntity1fields = append([]string{"acls"}, baseEntity2fields...)
)
