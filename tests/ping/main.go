package main

import (
	"fmt"
	"gocourse16/tests"
	"os"
	"os/signal"
)

const prefix = `----------------------`

func main() {
	conn := tests.CreateConn()
	defer conn.Close()

	err := conn.Ping()
	if err != nil {
		fmt.Printf("Ping error: %s\n", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for _ = range c {
		return
	}
}
