package extractor

import (
	"gocourse16/app/driver"
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// ExtractorStmt not completed extractor
	ExtractorStmt struct {
		stmt sqlparser.Statement
	}
)

func NewExtractorStmt(sql, defaultDb string) (driver.SqlPartsExtractor, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	res := &ExtractorStmt{stmt}

	return res, nil
}

func (s *ExtractorStmt) IsSelect() bool {
	if _, ok := s.stmt.(*sqlparser.Select); ok {
		return true
	}
	//if sstmt, ok := s.stmt.(*sqlparser.Insert); ok {
	//	if _, ok = sstmt.Rows.(*sqlparser.Select); ok {
	//		return true
	//	}
	//}

	return false
}

func (s *ExtractorStmt) UsedTables() []string {
	var tbls []string

	switch stmt := s.stmt.(type) { // TODO: parse all possible used tables and database
	case *sqlparser.Select:
		for i := range stmt.From {
			tbls = append(tbls, stmt.From[i].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String())
		}
	//case *sqlparser.Insert:
	//	return usedTables(stmt.Rows)
	default:
		panic("Not implemented yet")
	}

	return tbls
}
