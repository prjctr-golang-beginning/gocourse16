package column

import (
	"fmt"
	"strings"
)

type Nested struct {
	Interface
}

func (col *Nested) parse(t Type) (_ Interface, err error) {
	columns := fmt.Sprintf("Array(Tuple(%s))", strings.Join(nestedColumns(t.params()), ", "))
	if col.Interface, err = (&Array{}).parse(Type(columns)); err != nil {
		return nil, err
	}
	return col, nil
}

func nestedColumns(raw string) (columns []string) {
	var (
		begin    int
		brackets int
	)
	for i, r := range raw + "," {
		switch r {
		case '(':
			brackets++
		case ')':
			brackets--
		case ' ':
			if brackets == 0 {
				begin = i + 1
			}
		case ',':
			if brackets == 0 {
				columns, begin = append(columns, raw[begin:i]), i+1
				continue
			}
		}
	}
	for i, column := range columns {
		if strings.HasPrefix(column, "Nested(") {
			columns[i] = fmt.Sprintf("Array(Tuple(%s))", strings.Join(nestedColumns(Type(column).params()), ", "))
		}
	}
	return
}

var _ Interface = (*Nested)(nil)
