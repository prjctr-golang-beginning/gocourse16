package app

import (
	stringsStd "strings"
)

type (
	// Black/White tables list
	specialTables struct {
		Type SpecialTableType
		List []specialTable
	}
	specialTable struct {
		Name       string
		IsEntireDb bool
	}
)

var defaultBlackList = NewSpecialTables(BlackList, []string{`system.*`})

// NewSpecialTable construct special table in lower case
func NewSpecialTable(tName string) specialTable {
	t := stringsStd.ToLower(tName)
	return specialTable{
		Name:       stringsStd.Replace(t, `.*`, `.`, -1),
		IsEntireDb: stringsStd.HasSuffix(t, `.*`),
	}
}

// NewSpecialTables construct special tables list
func NewSpecialTables(tp SpecialTableType, tb []string) specialTables {
	tables := specialTables{Type: tp}
	for i := range tb {
		tables.List = append(tables.List, NewSpecialTable(tb[i]))
	}
	return tables
}

// ItIs checks is given table equal to this special table
func (s *specialTable) ItIs(tName string) bool {
	if s.IsEntireDb && stringsStd.HasPrefix(tName, s.Name) {
		return true
	}

	return tName == s.Name
}

// Contains checks is given table exists in list
func (s *specialTables) Contains(tName string) bool {
	for i := range s.List {
		if s.List[i].ItIs(tName) {
			return true
		}
	}

	return false
}

func (s *specialTables) IsWhite() bool {
	return s.Type == WhiteList
}

func (s *specialTables) IsBlack() bool {
	return s.Type == BlackList
}
