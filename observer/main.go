package main

import (
	"context"
	"github.com/maximorov/auditor"
	"observer/observer"
	"time"
)

type myRepository struct {
}

func (r *myRepository) CreateMany(ctx context.Context, enities []auditor.Valuable) (int, error) {
	return 3, nil
}

func main() {
	s := NewMyService(observer.NewObserversRegistrar(NewMyAuditor()))
	s.ExecuteImportantCommand(`one`)
	s.ExecuteImportantCommand(`two`)
	s.ExecuteImportantCommand(`three`)

	time.Sleep(time.Second * 5)
}
