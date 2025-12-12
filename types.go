package simplejsondb

import (
	"sync"
)

type db struct {
	useGzip bool
	path    string
}

type collection struct {
	useGzip   bool
	mu        sync.RWMutex
	name      string
	path      string
	recMu     sync.Mutex
	recModes  map[string]LockMode
	recLocks  map[string]*sync.RWMutex
	recStates map[string]*LockState
	recWg     map[string]*sync.WaitGroup
}

// LockMode is an enum for lock modes used by manual locking APIs.
type LockMode int

const (
	// NoMode represents no locking.
	NoMode LockMode = iota
	// ModeRead acquires a shared read lock.
	ModeRead
	// ModeWrite acquires an exclusive write lock.
	ModeWrite
	// ModeReadWrite is an alias for write (exclusive) lock.
	ModeReadWrite
)

// Options - extra configuration
type Options struct {
	UseGzip bool
}

// internal lock state tracking per ID to support safe unlock semantics
type LockState struct {
	R int // number of outstanding read locks acquired via LockID
	W int // number of outstanding write locks acquired via LockID (0 or 1)
}

// RecordLock combines the RWMutex and its LockState for a specific record ID.
type RecordLock struct {
	ID    string
	Lock  *sync.RWMutex
	State *LockState
	Mode  *LockMode
	WG    *sync.WaitGroup
}

// Collection - it's like a table name
type Collection interface {
	Get(string) ([]byte, error)
	GetAll() [][]byte
	GetAllByName() map[string][]byte
	Create(string, []byte, ...Options) error
	Delete(string) error
	Len() uint64
	LockID(id string, mode LockMode) (LockMode, error)
	UnlockID(id string) error
	GetLock(id string) *RecordLock
	IsLock(id string) bool
}

// DB - a database
type DB interface {
	Collection(string) (Collection, error)
}
