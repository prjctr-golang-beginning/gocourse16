package extractor

import (
	"fmt"
	"gocourse16/app/driver"
	"gocourse16/app/lib/strings"
	"regexp"
	strings2 "strings"
)

const (
	UndefinedExpression = iota
	SelectExpression
)

var (
	selectRegex   = regexp.MustCompile(`(?is)^(WITH.+AS\s(?:[a-zA-Z0-9_]+)|\))?\s?(?:SELECT)`)
	commentsRegex = regexp.MustCompile(`(\/\/.+$)|(\/\*(.|\n)*?\*\/)`)
	spacesRegex   = regexp.MustCompile(`(\s+$)|^(\s+)`)
	// TODO: solve problem with quotes
	tablesRegex = regexp.MustCompile(`(?is)(?:(?:FROM|JOIN)\s+([a-zA-Z0-9._]+)(?:,(?:\s+)?([a-zA-Z0-9._]+))*\s+)`)
	// TODO: solve problem with quotes
	tablesInRemoteRegex = regexp.MustCompile(`(?is)(?:remote(\s+)?\([^,]+,(?:\s+)?([a-zA-Z0-9_]+)(?:\s+)?(?:,|,)(?:\s+)?([a-zA-Z0-9_]+))`)
)

type (
	ExtractorRegex struct {
		query    string
		defDb    string
		exprType int8
	}
)

func NewExtractorRegex(sql, defaultDb string) (driver.SqlPartsExtractor, error) {
	query := sql
	query = commentsRegex.ReplaceAllString(sql, ``)
	query = spacesRegex.ReplaceAllString(query, ``)

	res := &ExtractorRegex{
		query:    query,
		defDb:    defaultDb,
		exprType: UndefinedExpression,
	}
	res.determineType()

	return res, nil
}

func (s *ExtractorRegex) determineType() {
	switch {
	case len(selectRegex.FindStringSubmatch(s.query)) > 0:
		s.exprType = SelectExpression
	}
}

func (s *ExtractorRegex) IsSelect() bool {
	return s.exprType == SelectExpression
}

// UsedTables returns slice of [database,table [, database,table]]
func (s *ExtractorRegex) UsedTables() []string {
	var tbls []string

	// from | join
	for _, found := range tablesRegex.FindAllStringSubmatch(s.query, -1) {
		if len(found) > 1 {
			for i := 1; i < len(found); i++ {
				if found[i] != `` {
					if !strings2.ContainsRune(found[i], '.') {
						found[i] = fmt.Sprintf("%s.%s", s.defDb, found[i])
					}
					tbls = append(tbls, found[i])
				}
			}
		}
	}

	// remote
	for _, found := range tablesInRemoteRegex.FindAllStringSubmatch(s.query, -1) {
		if len(found) > 1 {
			var (
				table    string
				database string
			)
			for i := 1; i < len(found); i++ {
				if found[i] != `` {
					switch database == `` {
					case true:
						database = found[i]
					case false:
						table = found[i]
					}
				}
			}
			tbls = append(tbls, fmt.Sprintf("%s.%s", database, table))
		}
		fmt.Printf("%s", found)
	}

	return strings.Unique(tbls)
}
