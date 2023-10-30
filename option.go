package table

type Option func(*helper)

func WithDBGeneratedField(col string) Option {
	return func(c *helper) {
		c.dbGeneratedFields = append(c.dbGeneratedFields, col)
	}
}
