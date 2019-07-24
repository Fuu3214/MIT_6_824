package raftkv

import "sync"

// KVDatabase should be db storing kv pairs,
// currently it stores kv pairs as go map datas tructure in memory
type KVDatabase struct {
	mu      sync.RWMutex
	Storage map[string]string
}

//Init a new kvdb
func InitDB() *KVDatabase {
	kvdb := new(KVDatabase)
	kvdb.Storage = make(map[string]string)
	return kvdb
}

// Put () replaces the value for a particular key in the database
func (kvdb *KVDatabase) Put(key string, value string) bool {
	kvdb.mu.Lock()
	defer kvdb.mu.Unlock()
	kvdb.Storage[key] = value
	return true
}

// Get () fetches the current value for a key
func (kvdb *KVDatabase) Get(key string) (string, bool) {
	kvdb.mu.RLock()
	defer kvdb.mu.RUnlock()
	value, ok := kvdb.Storage[key]
	return value, ok
}

// Append (key, arg) appends arg to key's value
// An Append to a non-existant key should act like Put
func (kvdb *KVDatabase) Append(key string, arg string) bool {
	kvdb.mu.Lock()
	defer kvdb.mu.Unlock()
	kvdb.Storage[key] = kvdb.Storage[key] + arg
	return true
}
