package extractor

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	selectOk1       = "/*some comments*/ \n\tSELECT method, host, path FROM table1 limit 10\nUNION ALL\nSELECT method, /*host*/, path FROM table2 limit 10 // line comments"
	selectOk1TNames = []string{`default.table1`, `default.table1`}
	selectOk2       = "SELECT order_id From /*wrong table name*/ remote('host1', default, missed_orders) limit 100\n"
	selectOk2TNames = []string{`default.missed_orders`}
	selectOk3       = "Select     orderId, articleId FROM   mark_store_reserve_as_deleted t1\n INNER JOIN remote('host1', default, missed_orders) t2 ON t1.orderId = t2.order_id\nLIMIT 100"
	selectOk3TNames = []string{`default.mark_store_reserve_as_deleted`, `default.missed_orders`}
	selectOk4       = "WITH ( \n   SELECT sum(bytes_on_disk)\n  FROM system.parts\n   WHERE active AND database NOT IN (1,2,3)     \n) AS max\nSELECT\n    database,\n    table,\n    sum(bytes_on_disk) AS b,\n    round((b / max) * 100, 2) AS weight\nFROM system.parts WHERE active AND database NOT IN (....) [ OR ((database != AND table !=) ... ]\nGROUP BY\n    database,\n    table\nORDER BY b ASC"
	selectOk4TNames = []string{`system.parts`}

	selectNotOk1 = "Insert INTO t1 SELECT FROM some_table"
	selectNotOk2 = "UPDATE t3 (v1,v2,v3) VALUES (SELECT FROM /*wrong table name*/ remote('host1', default, missed_orders) limit 100)\n"
)

func TestExtractorRegex_IsSelect(t *testing.T) {
	for _, q := range []string{selectOk1, selectOk2, selectOk3, selectOk4} {
		e, err := NewExtractorRegex(q, `default`)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, true, e.IsSelect())
	}

	for _, q := range []string{selectNotOk1, selectNotOk2} {
		e, err := NewExtractorRegex(q, `default`)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, false, e.IsSelect())
	}
}

func TestExtractorRegex_UsedTables(t *testing.T) {
	e, err := NewExtractorRegex(selectOk1, `default`)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, selectOk1TNames, e.UsedTables())

	e, err = NewExtractorRegex(selectOk2, `default`)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, selectOk2TNames, e.UsedTables())

	e, err = NewExtractorRegex(selectOk3, `default`)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, selectOk3TNames, e.UsedTables())

	e, err = NewExtractorRegex(selectOk4, `default`)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, selectOk4TNames, e.UsedTables())
}
