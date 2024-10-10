package memoize

import (
	"container/list"
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

type cacheEntry struct {
	key      string
	result   []reflect.Value
	ready    chan struct{}
	expireAt time.Time
	element  *list.Element
}

type Option func(*memoizeOptions)

type memoizeOptions struct {
	maxSize    int
	expiration time.Duration
}

func WithMaxSize(size int) Option {
	return func(o *memoizeOptions) {
		o.maxSize = size
	}
}

func WithExpiration(d time.Duration) Option {
	return func(o *memoizeOptions) {
		o.expiration = d
	}
}

// Memoize takes a function of any type and returns a memoized version of it.
func Memoize[F any](f F, options ...Option) F {
	ft := reflect.TypeOf(f)
	if ft.Kind() != reflect.Func {
		panic("Memoize: argument must be a function")
	}

	opts := memoizeOptions{
		maxSize:    100,       // Default max size
		expiration: time.Hour, // Default expiration
	}
	for _, option := range options {
		option(&opts)
	}

	cache := &sync.Map{}
	lru := list.New()
	var mutex sync.Mutex

	cleanup := func() {
		mutex.Lock()
		defer mutex.Unlock()

		now := time.Now()
		for lru.Len() > 0 {
			oldest := lru.Back()
			entry := oldest.Value.(*cacheEntry)
			if now.After(entry.expireAt) || lru.Len() > opts.maxSize {
				lru.Remove(oldest)
				cache.Delete(entry.key)
			} else {
				break
			}
		}
	}

	go func() {
		for {
			time.Sleep(opts.expiration / 10)
			cleanup()
		}
	}()

	wrapped := reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
		key := makeKey(args)

		now := time.Now()
		if value, found := cache.Load(key); found {
			entry := value.(*cacheEntry)
			if now.Before(entry.expireAt) {
				<-entry.ready // Wait for the result to be ready
				mutex.Lock()
				lru.MoveToFront(entry.element)
				mutex.Unlock()
				return entry.result
			}
			// Entry has expired, remove it
			mutex.Lock()
			lru.Remove(entry.element)
			cache.Delete(entry.key)
			mutex.Unlock()
		}

		// Compute the result
		entry := &cacheEntry{
			key:      key,
			ready:    make(chan struct{}),
			expireAt: now.Add(opts.expiration),
		}
		if actual, loaded := cache.LoadOrStore(key, entry); loaded {
			entry = actual.(*cacheEntry)
			<-entry.ready // Wait for the result to be ready
			if now.Before(entry.expireAt) {
				return entry.result
			}
			// Entry has expired, remove it and recompute
			mutex.Lock()
			lru.Remove(entry.element)
			cache.Delete(entry.key)
			mutex.Unlock()
		} else {
			defer close(entry.ready) // Signal that the result is ready
			entry.result = reflect.ValueOf(f).Call(args)

			mutex.Lock()
			if lru.Len() >= opts.maxSize {
				oldest := lru.Back()
				oldestEntry := oldest.Value.(*cacheEntry)
				lru.Remove(oldest)
				cache.Delete(oldestEntry.key)
			}
			entry.element = lru.PushFront(entry)
			mutex.Unlock()

			return entry.result
		}

		// Recompute if entry was expired
		newEntry := &cacheEntry{
			key:      key,
			ready:    make(chan struct{}),
			expireAt: now.Add(opts.expiration),
		}
		cache.Store(key, newEntry)
		defer close(newEntry.ready)
		newEntry.result = reflect.ValueOf(f).Call(args)

		mutex.Lock()
		if lru.Len() >= opts.maxSize {
			oldest := lru.Back()
			oldestEntry := oldest.Value.(*cacheEntry)
			lru.Remove(oldest)
			cache.Delete(oldestEntry.key)
		}
		newEntry.element = lru.PushFront(newEntry)
		mutex.Unlock()

		return newEntry.result
	})

	return wrapped.Interface().(F)
}

func makeKey(args []reflect.Value) string {
	var key strings.Builder
	for i, arg := range args {
		if i > 0 {
			key.WriteString(",")
		}
		if true {
			key.WriteString(makeShallowKey(arg))
		} else {
			key.WriteString(makeDeepKey(arg))
		}
	}
	return key.String()
}

// in case we want to start using a deep key
func makeDeepKey(v reflect.Value) string {
	var key string
	switch v.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map, reflect.Struct:
		// Use a string representation for complex types
		key += fmt.Sprintf("%#v", v.Interface())
	default:
		// Use the value directly for simple types
		key += fmt.Sprintf("%v", v.Interface())
	}
	return key
}

func makeShallowKey(v reflect.Value) string {
	// Special handling for context.Context
	if v.Type().Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		return "context" // We return a constant string for all contexts
	}

	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return "nil"
		}
		return fmt.Sprintf("ptr(%p):%s", v.Interface(), makeShallowKey(v.Elem()))
	case reflect.Struct:
		var fieldKeys []string
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if t.Field(i).IsExported() {
				field := v.Field(i)
				fieldKeys = append(fieldKeys, fmt.Sprintf("%s:%s", t.Field(i).Name, makeShallowKey(field)))
			}
		}
		return fmt.Sprintf("%s{%s}", v.Type().Name(), strings.Join(fieldKeys, ","))
	case reflect.Slice:
		var elemKeys []string
		for i := 0; i < v.Len(); i++ {
			elemKeys = append(elemKeys, fmt.Sprintf("%v", v.Index(i).Interface()))
		}
		return fmt.Sprintf("%s[%s]", v.Type().Name(), strings.Join(elemKeys, ","))
	case reflect.Map:
		// For maps, we still use length and address to avoid deep comparison
		return fmt.Sprintf("%s(len=%d,addr=%p)", v.Type().Name(), v.Len(), v.Interface())
	default:
		return fmt.Sprintf("%v", v.Interface())
	}
}
