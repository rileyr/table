package table_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/rileyr/table"
	"github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {

	type user struct {
		ID        int          `db:"id"`
		CreatedAt sql.NullTime `db:"created_at"`
		UpdatedAt sql.NullTime `db:"updated_at"`
		Username  string       `db:"username"`
		Email     string       `db:"email"`
	}

	subject := table.New[user](
		"users", "id",
		table.WithDBGeneratedField("id"),
		table.WithDBGeneratedField("created_at"),
		table.WithDBGeneratedField("updated_at"),
	)

	t.Run("Fetch", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer mockDB.Close()
		db := sqlx.NewDb(mockDB, "sqlmock")

		now := sql.NullTime{Time: time.Now(), Valid: true}
		rows := sqlmock.NewRows([]string{"id", "created_at", "updated_at", "username", "email"}).AddRow(
			40, now, now, "foobar", "foo@bar.bar",
		)

		mock.
			ExpectQuery("SELECT created_at, email, id, updated_at, username FROM users WHERE username = ?").
			WithArgs("foobar").
			WillReturnRows(rows)

		result, err := subject.Fetch(db, "username", "foobar")

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
		assert.Equal(t, "foobar", result.Username)
		assert.Equal(t, "foo@bar.bar", result.Email)
		assert.Equal(t, now, result.CreatedAt)
		assert.Equal(t, now, result.UpdatedAt)
	})

	t.Run("Select", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer mockDB.Close()
		db := sqlx.NewDb(mockDB, "sqlmock")

		now := sql.NullTime{Time: time.Now(), Valid: true}
		rows := sqlmock.NewRows([]string{"id", "created_at", "updated_at", "username", "email"}).
			AddRow(40, now, now, "foobar", "foo@bar.bar").
			AddRow(41, now, now, "foobiz", "foo@biz.biz").
			AddRow(42, now, now, "foobang", "foo@bang.bang")

		mock.
			ExpectQuery("SELECT created_at, email, id, updated_at, username FROM users WHERE username IS NULL").
			WillReturnRows(rows)

		result, err := subject.Select(db, "username IS NULL")

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.NoError(t, mock.ExpectationsWereMet())
		assert.Equal(t, 3, len(result))
		assert.Equal(t, 40, result[0].ID)
		assert.Equal(t, 41, result[1].ID)
		assert.Equal(t, 42, result[2].ID)
	})

	t.Run("Insert", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer mockDB.Close()
		db := sqlx.NewDb(mockDB, "sqlmock")

		now := sql.NullTime{Time: time.Now(), Valid: true}

		rows := sqlmock.NewRows([]string{"id", "created_at", "updated_at"}).
			AddRow(40, now, now)

		mock.
			ExpectQuery("INSERT INTO users(email, username) VALUES (?, ?) RETURNING created_at, id, updated_at").
			WithArgs("email@test.com", "foobar").
			WillReturnRows(rows)

		obj := user{
			Username: "foobar",
			Email:    "email@test.com",
		}

		assert.NoError(t, subject.Insert(db, &obj))
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
		assert.Equal(t, 40, obj.ID)
	})

	t.Run("Replace", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer mockDB.Close()
		db := sqlx.NewDb(mockDB, "sqlmock")

		now := sql.NullTime{Time: time.Now(), Valid: true}
		updated := sql.NullTime{Time: now.Time.Add(time.Minute), Valid: true}

		rows := sqlmock.NewRows([]string{"updated_at"}).
			AddRow(updated)

		mock.
			ExpectQuery("UPDATE users SET email = ?, username = ? WHERE id = ? RETURNING created_at, id, updated_at").
			WillReturnRows(rows)

		user := user{Username: "newname", Email: "newemail@test.com", ID: 40}
		assert.NoError(t, subject.Replace(db, &user))
		assert.NoError(t, mock.ExpectationsWereMet())
		assert.Equal(t, updated.Time.String(), user.UpdatedAt.Time.String())
	})

	t.Run("Delete", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer mockDB.Close()
		db := sqlx.NewDb(mockDB, "sqlmock")

		mock.
			ExpectExec("DELETE FROM users WHERE email = ?").
			WithArgs("email@test.com").WillReturnResult(sqlmock.NewResult(0, 55))

		count, err := subject.Delete(db, "email = ?", "email@test.com")
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
		assert.Equal(t, int64(55), count)
	})
}
