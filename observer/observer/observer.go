package observer

type Subject interface {
	Register(Observer)
	Deregister(Observer)
	Notify(any)
}

type Observer interface {
	Update(any)
	GetID() string
}

func NewObserversRegistrar(os ...Observer) *ObserversRegistrar {
	or := &ObserversRegistrar{}

	for i := range os {
		or.Register(os[i])
	}

	return or
}

// ObserversRegistrar NONE THREAD SAFE
type ObserversRegistrar struct {
	_observers []Observer
}

func (r *ObserversRegistrar) Register(o Observer) {
	r._observers = append(r._observers, o)
}

func (r *ObserversRegistrar) Deregister(o Observer) {
	r._observers = removeFromSlice(r._observers, o)
}

func (r *ObserversRegistrar) Notify(s any) {
	for i := range r._observers {
		r._observers[i].Update(s)
	}
}

func removeFromSlice(observerList []Observer, observerToRemove Observer) []Observer {
	observerListLength := len(observerList)
	for i, observer := range observerList {
		if observerToRemove.GetID() == observer.GetID() {
			observerList[observerListLength-1], observerList[i] = observerList[i], observerList[observerListLength-1]
			return observerList[:observerListLength-1]
		}
	}
	return observerList
}
