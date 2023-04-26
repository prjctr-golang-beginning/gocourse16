package main

import (
	"github.com/maximorov/auditor"
)

type MyAuditor struct {
	*auditor.Auditor
}

func (a *MyAuditor) GetID() string {
	return `auditor`
}

func NewMyAuditor() *MyAuditor {
	return &MyAuditor{auditor.New(&myRepository{})}
}
