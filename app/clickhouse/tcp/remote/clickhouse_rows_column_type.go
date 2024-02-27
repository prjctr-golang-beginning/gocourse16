package remote

import (
	"gocourse16/app/clickhouse/tcp/lib/column"
	"gocourse16/app/clickhouse/tcp/lib/driver"
	"reflect"
)

type columnType struct {
	name     string
	chType   string
	nullable bool
	scanType reflect.Type
}

func (c *columnType) Name() string {
	return c.name
}

func (c *columnType) Nullable() bool {
	return c.nullable
}

func (c *columnType) ScanType() reflect.Type {
	return c.scanType
}

func (c *columnType) DatabaseTypeName() string {
	return c.chType
}

func (r *rows) ColumnTypes() []driver.ColumnType {
	types := make([]driver.ColumnType, 0, len(r.columns))
	for i, c := range r.block.Columns {
		_, nullable := c.(*column.Nullable)
		types = append(types, &columnType{
			name:     r.columns[i],
			chType:   string(c.Type()),
			nullable: nullable,
			scanType: c.ScanType(),
		})
	}
	return types
}
