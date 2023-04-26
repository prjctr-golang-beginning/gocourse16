package main

import (
	"context"
	"github.com/maximorov/auditor"
)

type MyAuditor struct {
	*auditor.Auditor
}

func (a *MyAuditor) Name() string {
	return `auditor`
}

func (a *MyAuditor) Notify(_ context.Context, body any, _ chan error) {
	a.Update(body)
}

func (a *MyAuditor) Close() error {
	return nil
}

func NewMyAuditor() *MyAuditor {
	return &MyAuditor{auditor.New(&myRepository{})}
}
