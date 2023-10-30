package table

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// Table provides boilerplate ops for some table with sqlx.
type Table[T any] struct {
	name string
	pk   string

	helper *Helper
}

// New returns a new Table for the given type.
func New[T any](name, pk string, opts ...HelperOption) *Table[T] {
	var t T
	return &Table[T]{
		helper: NewHelper(t, opts...),
		name:   name,
		pk:     pk,
	}
}

// Fetch returns a single record with a value matching a given field. Mostly
// useful for fetching by unique values.
func (t *Table[T]) Fetch(ctx context.Context, db *sqlx.DB, col string, val any) (*T, error) {
	q := "SELECT " + t.helper.All + " FROM " + t.name + " WHERE " + col + " = ?"
	rows, err := db.QueryxContext(ctx, q, val)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var obj T
	for rows.Next() {
		if err := rows.StructScan(&obj); err != nil {
			return nil, err
		}
		break
	}
	return &obj, nil
}

// Select fetches all records matching a given query.
func (t *Table[T]) Select(ctx context.Context, db *sqlx.DB, query string, args ...any) ([]*T, error) {
	q := "SELECT " + t.helper.All + " FROM " + t.name + " WHERE " + query
	rows, err := db.QueryxContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var col []*T
	for rows.Next() {
		var row T
		if err := rows.StructScan(&row); err != nil {
			return nil, err
		}
		col = append(col, &row)
	}

	return col, nil
}

// Insert inserts a single record into the table.
func (t *Table[T]) Insert(ctx context.Context, db *sqlx.DB, obj *T) error {
	h := t.helper
	q := "INSERT INTO " + t.name + "(" + h.Inserts + ") VALUES (" + h.InsertFields + ") RETURNING " + h.Returning
	rows, err := db.NamedQueryContext(ctx, q, obj)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.StructScan(obj); err != nil {
			return err
		}
		break
	}

	return nil
}

// Replace replaces the given record.
func (t *Table[T]) Replace(ctx context.Context, db *sqlx.DB, obj *T) error {
	q := "UPDATE " + t.name + " SET " + t.helper.UpdateValues + " WHERE " + t.pk + " = :" + t.pk + " RETURNING " + t.helper.Returning
	rows, err := db.NamedQueryContext(ctx, q, obj)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.StructScan(obj); err != nil {
			return err
		}
		break
	}

	return nil
}

// Delete deletes records matching the query.
func (t *Table[T]) Delete(ctx context.Context, db *sqlx.DB, query string, args ...any) (int64, error) {
	q := "DELETE FROM " + t.name + " WHERE " + query
	res, err := db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
