package main

import (
	"context"
	"github.com/maximorov/auditor"
	"pubsub/pubsub"
	"time"
)

type myRepository struct {
}

func (r *myRepository) CreateMany(ctx context.Context, enities []auditor.Valuable) (int, error) {
	return 3, nil
}

func main() {
	ctx := context.Background()

	a := NewMyAuditor()
	p := pubsub.NewPublisher(ctx)
	p.AddSubscriber(a)

	s := NewMyService(p)
	s.ExecuteImportantCommand(`one`)
	s.ExecuteImportantCommand(`two`)
	s.ExecuteImportantCommand(`three`)

	time.Sleep(time.Second * 5)
}
