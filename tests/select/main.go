package main

import (
	"fmt"
	"gocourse16/tests"
)

const prefix = `-----------------`

func main() {
	conn := tests.CreateConn()

	rows, err := conn.Query("SELECT column1 FROM table1 limit 50")
	fmt.Printf(prefix+"Err: %s, Rows: %v\n", err, rows)
	cols, err := rows.Columns()
	fmt.Printf(prefix+"Err: %s, Cols: %v\n\n", err, len(cols))
	var (
		order_id int
	)
	if err == nil {
		colsNum := 0
		for rows.Next() {
			colsNum++
			if err := rows.Scan(&order_id); err != nil {
				fmt.Printf("Err: %s\n", err)
			}
			if order_id == 0 {
				fmt.Printf(prefix + "NOT PASS!!!\n")
			} else {
				fmt.Printf(prefix+"Order ID:%d\n", order_id)
			}
		}
		rows.Close()
		fmt.Printf(prefix+"Cols: %d\n\n\n", colsNum)
	}
	conn.Close()
}
