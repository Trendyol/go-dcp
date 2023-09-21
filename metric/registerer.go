package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Registerer struct {
	wrap prometheus.Registerer
	cs   map[prometheus.Collector]struct{}
}

func WrapWithRegisterer(reg prometheus.Registerer) *Registerer {
	return &Registerer{
		wrap: reg,
		cs:   make(map[prometheus.Collector]struct{}),
	}
}

func (u *Registerer) Register(c prometheus.Collector) error {
	if u.wrap == nil {
		return nil
	}

	err := u.wrap.Register(c)
	if err != nil {
		return err
	}
	u.cs[c] = struct{}{}
	return nil
}

func (u *Registerer) Unregister(c prometheus.Collector) bool {
	if u.wrap != nil && u.wrap.Unregister(c) {
		delete(u.cs, c)
		return true
	}
	return false
}

func (u *Registerer) UnregisterAll() bool {
	success := true
	for c := range u.cs {
		if !u.Unregister(c) {
			success = false
		}
	}
	return success
}

func (u *Registerer) RegisterAll(cs []prometheus.Collector) error {
	var result error
	for i := range cs {
		err := u.Register(cs[i])
		if err != nil {
			result = err
		}
	}

	return result
}
