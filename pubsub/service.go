package main

import (
	"context"
	"pubsub/pubsub"
)

type MyService struct {
	importantThing string
	pubsub.Publisher
}

func (s *MyService) ExecuteImportantCommand(someArgument string) {
	s.importantThing = someArgument
	s.Publish(context.Background(), s, nil)
}

func (s *MyService) Value() any {
	return s.importantThing
}

func NewMyService(os pubsub.Publisher) *MyService {
	return &MyService{Publisher: os}
}
