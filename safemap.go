package utils4go

import (
	"sync"
)

type SafeMap struct {
	Lock		*sync.RWMutex
	Data		map[string]interface{}
}

func NewSafeMap() *SafeMap{
	return &SafeMap{
		Lock:new(sync.RWMutex),
		Data:make(map[string]interface{}),
	}
}

func (m *SafeMap)Get(k string) (interface{}, bool) {
	m.Lock.RLock()
	defer m.Lock.RUnlock()
	v, ok := m.Data[k]
	return v,ok
}

func (m *SafeMap)Set(k string, v interface{}) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	m.Data[k] = v
}

func (m *SafeMap) Delete(k string) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	delete(m.Data, k)
}

func (m *SafeMap) Clear() {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	m.Data = make(map[string]interface{})
}

func (m *SafeMap) Foreach(f func(string, interface{})) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	for k, v := range m.Data {
		f(k,v)
	}
}