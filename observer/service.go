package main

import "observer/observer"

type MyService struct {
	importantThing string
	*observer.ObserversRegistrar
}

func (s *MyService) ExecuteImportantCommand(someArgument string) {
	s.importantThing = someArgument
	s.Notify(s)
}

func (s *MyService) Value() any {
	return s.importantThing
}

func NewMyService(os *observer.ObserversRegistrar) *MyService {
	return &MyService{ObserversRegistrar: os}
}
