package simplejsondb

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	Ext            string = ".json"
	GZipExt        string = ".json.gz"
	ErrNoDirectory error  = errors.New("not a directory")
)

const (
	// ModeRead acquires a shared read lock.
	ModeRead LockMode = iota
	// ModeWrite acquires an exclusive write lock.
	ModeWrite
	// ModeReadWrite is an alias for write (exclusive) lock.
	ModeReadWrite
)

type (
	// Options - extra configuration
	Options struct {
		UseGzip bool
	}

	db struct {
		useGzip bool
		path    string
	}

	collection struct {
		useGzip   bool
		mu        sync.RWMutex
		name      string
		path      string
		recMu     sync.Mutex
		recLocks  map[string]*sync.RWMutex
		recStates map[string]*lockState
	}
	// LockMode is an enum for lock modes used by manual locking APIs.
	LockMode int
)

// internal lock state tracking per ID to support safe unlock semantics
type lockState struct {
	r int // number of outstanding read locks acquired via LockID
	w int // number of outstanding write locks acquired via LockID (0 or 1)
}

type (
	// Collection - it's like a table name
	Collection interface {
		Get(string) ([]byte, error)
		GetAll() [][]byte
		GetAllByName() map[string][]byte
		Create(string, []byte, ...Options) error
		Delete(string) error
		Len() uint64
		LockID(id string, mode LockMode)
		UnlockID(id string, mode LockMode)
	}
	// DB - a database
	DB interface {
		Collection(string) (Collection, error)
	}
)

// New - a database instance
func New(dbname string, options *Options) (DB, error) {
	opts := Options{}
	if options != nil {
		opts = *options
	}

	dbpath := filepath.Join(dbname)
	_, err := getOrCreateDir(dbpath)
	if err != nil {
		return nil, err
	}

	return &db{path: dbpath, useGzip: opts.UseGzip}, nil
}

// Collection returns the collection or table
func (db *db) Collection(name string) (Collection, error) {
	c := filepath.Join(db.path, name)
	dir, err := getOrCreateDir(c)
	if err != nil {
		return nil, err
	}
	if !dir.IsDir() {
		return nil, ErrNoDirectory
	}
	return &collection{name: name, path: c, useGzip: db.useGzip}, nil
}

// GetAll - returns all records
func (c *collection) GetAll() (data [][]byte) {
	records, err := os.ReadDir(c.path)
	if err != nil {
		return
	}
	for _, r := range records {
		if !r.IsDir() {
			fPath := filepath.Join(c.path, r.Name())
			record, err := os.ReadFile(fPath)
			if err != nil {
				continue // skipping a file which has issue
			}

			if strings.LastIndex(r.Name(), GZipExt) > 0 {
				record, _ = UnGzip(record) // skipping ungip error over mutli file fetch
			}

			data = append(data, record)
		}
	}
	return
}

// GetAllByName - returns all records
func (c *collection) GetAllByName() (data map[string][]byte) {
	data = make(map[string][]byte)

	records, err := os.ReadDir(c.path)
	if err != nil {
		return
	}
	for _, r := range records {
		if !r.IsDir() {
			fPath := filepath.Join(c.path, r.Name())
			record, err := os.ReadFile(fPath)
			if err != nil {
				continue // skipping a file which has issue
			}

			if strings.LastIndex(r.Name(), GZipExt) > 0 {
				record, _ = UnGzip(record) // skipping ungip error over mutli file fetch
			}

			name := strings.TrimSuffix(r.Name(), Ext)
			data[name] = record
		}
	}
	return
}

// Get help to retrive key based record
func (c *collection) Get(key string) (data []byte, err error) {
	filename, err, isGzip := c.getPathIfExist(key, err)
	data, err = os.ReadFile(filename)
	if err != nil {
		return
	}

	if isGzip {
		_data, _err := UnGzip(data)
		if _err != nil {
			return _data, _err
		}
		data, err = _data, _err
	}
	return
}

// Insert - helps to save data into model dir
func (c *collection) Create(key string, data []byte, options ...Options) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var useGzip bool = c.useGzip
	if !c.useGzip {
		if options != nil && options[0].UseGzip {
			useGzip = options[0].UseGzip
		}
	}
	filename := c.getFullPath(key, c.useGzip)
	if useGzip {
		data, err = Gzip(data)
		if err != nil {
			return err
		}
	}
	return os.WriteFile(filename, data, os.ModePerm)
}

// Delete - helps to delete model dir record
func (c *collection) Delete(key string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	filename, err, _ := c.getPathIfExist(key, err)
	if err != nil {
		return err
	}

	return os.Remove(filename)
}

func (c *collection) Len() uint64 {
	records, _ := os.ReadDir(c.path)
	return uint64(len(records))
}

func getOrCreateDir(path string) (os.FileInfo, error) {
	f, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			cwd, err := os.Getwd()
			if err != nil {
				return nil, err
			}
			newDir := filepath.Join(cwd, path)
			err = os.Mkdir(filepath.Join(cwd, path), os.ModePerm)
			if err != nil {
				return nil, err
			}
			return os.Stat(newDir)
		}
		return f, err
	}
	return f, nil
}

func (c *collection) getFullPath(key string, isGzip bool) string {
	var record string
	if isGzip {
		record = key + GZipExt
	} else {
		record = key + Ext
	}
	filename := filepath.Join(c.path, record)

	return filename
}

func (c *collection) getPathIfExist(key string, err error) (string, error, bool) {
	record := key + Ext
	filename := filepath.Join(c.path, record)

	if success, err := c.isExist(filename, err); !success {
		record = key + GZipExt
		filename = filepath.Join(c.path, record)
		if success, err := c.isExist(filename, err); !success {
			return "", err, false
		}

		return filename, nil, true
	}

	return filename, nil, false
}

func (c *collection) isExist(filename string, err error) (bool, error) {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false, err
	}
	if !info.IsDir() {
		return true, nil
	}
	return false, nil
}

func UnGzip(record []byte) (result []byte, err error) {
	var buffer bytes.Buffer
	_, err = buffer.Write(record)
	if err != nil {
		return record, err
	}
	reader, err := gzip.NewReader(&buffer)

	result, err = io.ReadAll(reader)
	if err != nil {
		return record, err
	}

	err = reader.Close()
	if err != nil {
		return record, nil
	}

	return
}

func Gzip(data []byte) (result []byte, err error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, err = writer.Write(data)
	if err != nil {
		return data, err
	}
	err = writer.Close()
	result = buffer.Bytes()
	return result, err
}

// helper: returns the RWMutex for a specific record ID, creating it if needed
func (c *collection) getRecordLock(id string) *sync.RWMutex {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recLocks == nil {
		c.recLocks = make(map[string]*sync.RWMutex)
	}
	l, ok := c.recLocks[id]
	if !ok || l == nil {
		l = &sync.RWMutex{}
		c.recLocks[id] = l
	}
	return l
}

// helper: returns the RWMutex for a specific record ID if it exists; does not create it
func (c *collection) getRecordLockIfExists(id string) *sync.RWMutex {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recLocks == nil {
		return nil
	}
	return c.recLocks[id]
}

// helper: returns the lockState for a specific ID, creating it if needed
func (c *collection) getOrCreateState(id string) *lockState {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recStates == nil {
		c.recStates = make(map[string]*lockState)
	}
	st, ok := c.recStates[id]
	if !ok || st == nil {
		st = &lockState{}
		c.recStates[id] = st
	}
	return st
}

// helper: returns the lockState for a specific ID if it exists; does not create it
func (c *collection) getStateIfExists(id string) *lockState {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recStates == nil {
		return nil
	}
	return c.recStates[id]
}

// LockID allows manual locking for a specific record ID.
func (c *collection) LockID(id string, mode LockMode) {
	l := c.getRecordLock(id)
	st := c.getOrCreateState(id)
	switch mode {
	case ModeRead:
		st.r++
		l.RLock()
	case ModeReadWrite:
		st.r++
		st.w++
		l.Lock()
	default: // write/read_write/other (exclusive)
		st.w++
		l.Lock()
	}
}

// UnlockID releases a previously acquired lock for a specific record ID.
// mode should match the mode used in LockID.
func (c *collection) UnlockID(id string, mode LockMode) {
	// Do not create a new lock when unlocking; if lock/state don't exist, it's a no-op
	l := c.getRecordLockIfExists(id)
	st := c.getStateIfExists(id)
	if l == nil || st == nil {
		return
	}
	switch mode {
	case ModeRead:
		if st.r <= 0 {
			panic("double unlock: read lock not held")
		}
		st.r--
		l.RUnlock()
	case ModeReadWrite:
		if st.r <= 0 {
			panic("double unlock: read lock not held")
		}
		if st.w <= 0 {
			panic("double unlock: write lock not held")
		}
		st.r--
		st.w--
		l.Unlock()
	default: // write/read_write/other (exclusive)
		if st.w <= 0 {
			panic("double unlock: write lock not held")
		}
		st.w--
		l.Unlock()
	}
}
