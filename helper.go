package table

import (
	"sort"
	"strings"

	"github.com/fatih/structs"
)

type Helper struct {
	All          string
	Inserts      string
	InsertFields string
	Returning    string
	UpdateValues string

	dbGeneratedFields []string
}

func NewHelper(obj any, opts ...HelperOption) *Helper {
	c := Helper{}
	for _, opt := range opts {
		opt(&c)
	}

	all := []string{}

	fields := structs.Fields(obj)
	for _, f := range fields {
		tag := f.Tag("db")
		if tag == "-" || tag == "" {
			continue
		}
		all = append(all, tag)
	}

	sort.Strings(all)
	sort.Strings(c.dbGeneratedFields)

	inserts := []string{}
	for _, col := range all {
		insertable := true
		for _, skip := range c.dbGeneratedFields {
			if skip == col {
				insertable = false
				break
			}
		}
		if insertable {
			inserts = append(inserts, col)
		}
	}

	ifs := make([]string, len(inserts))
	ufs := make([]string, len(inserts))
	for i := range ifs {
		ifs[i] = ":" + inserts[i]
		ufs[i] = inserts[i] + " = :" + inserts[i]
	}

	c.All = strings.Join(all, ", ")
	c.Inserts = strings.Join(inserts, ", ")
	c.InsertFields = strings.Join(ifs, ", ")
	c.UpdateValues = strings.Join(ufs, ", ")
	c.Returning = strings.Join(c.dbGeneratedFields, ", ")
	return &c
}

type HelperOption func(*Helper)

func WithDBGeneratedField(col string) HelperOption {
	return func(c *Helper) {
		c.dbGeneratedFields = append(c.dbGeneratedFields, col)
	}
}
