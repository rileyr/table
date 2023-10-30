package table_test

import (
	"testing"
	"time"

	"github.com/rileyr/table"
	"github.com/stretchr/testify/assert"
)

func TestHelper(t *testing.T) {
	type foo struct {
		Bar       int       `db:"bar"`
		ID        int       `db:"id"`
		CreatedAt time.Time `db:"created"`
		Whiz      string    `db:"whiz"`
		Skip      bool      `db:"-"`
		Hello     any       `db:"there"`
	}

	helper := table.NewHelper(
		foo{},
		table.WithDBGeneratedField("id"),
		table.WithDBGeneratedField("created"),
	)

	assert.NotNil(t, helper)
	assert.Equal(t, "bar, created, id, there, whiz", helper.All)
	assert.Equal(t, "bar, there, whiz", helper.Inserts)
	assert.Equal(t, ":bar, :there, :whiz", helper.InsertFields)
	assert.Equal(t, "created, id", helper.Returning)
	assert.Equal(t, "bar = :bar, there = :there, whiz = :whiz", helper.UpdateValues)
}
