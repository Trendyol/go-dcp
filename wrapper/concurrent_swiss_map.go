package wrapper

import (
	jsoniter "github.com/json-iterator/go"
	csmap "github.com/mhmtszr/concurrent-swiss-map"
)

type ConcurrentSwissMap[K comparable, V any] struct {
	m *csmap.CsMap[K, V]
}

func CreateConcurrentSwissMap[K comparable, V any](size uint64) *ConcurrentSwissMap[K, V] {
	return &ConcurrentSwissMap[K, V]{
		m: csmap.Create[K, V](
			csmap.WithSize[K, V](size),
		),
	}
}

func (m *ConcurrentSwissMap[K, V]) Delete(key K) {
	m.m.Delete(key)
}

func (m *ConcurrentSwissMap[K, V]) Load(key K) (value V, ok bool) {
	return m.m.Load(key)
}

func (m *ConcurrentSwissMap[K, V]) Range(f func(key K, value V) bool) {
	m.m.Range(func(key K, value V) bool {
		return !f(key, value)
	})
}

func (m *ConcurrentSwissMap[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}

func (m *ConcurrentSwissMap[K, V]) Count() int {
	return m.m.Count()
}

func (m *ConcurrentSwissMap[K, V]) ToMap() map[K]V {
	result := make(map[K]V)
	m.Range(func(key K, value V) bool {
		result[key] = value
		return false
	})
	return result
}

func (m *ConcurrentSwissMap[K, V]) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(m.ToMap())
}

func (m *ConcurrentSwissMap[K, V]) UnmarshalJSON(data []byte) error {
	var result map[K]V
	if err := jsoniter.Unmarshal(data, &result); err != nil {
		return err
	}

	for k, v := range result {
		m.Store(k, v)
	}

	return nil
}
