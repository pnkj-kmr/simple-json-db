package simplejsondb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	zrl "github.com/pnkj-kmr/zap-rotate-logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const _ext string = ".json"

type (
	// Logger - logging interface
	Logger interface {
		Error(string, ...zapcore.Field)
		Warn(string, ...zapcore.Field)
		Info(string, ...zapcore.Field)
		Debug(string, ...zapcore.Field)
	}

	// Collection - it's like a table name
	Collection interface {
		Get(string) ([]byte, error)
		GetAll() [][]byte
		Create(string, []byte) error
		Delete(string) error
	}
	// DB - a database
	DB interface {
		Collection(string) (Collection, error)
	}
)

type (
	// Options - extra configuration
	Options struct {
		Logger
	}

	_db struct {
		path   string
		logger Logger
	}

	_collection struct {
		mu     sync.Mutex
		name   string
		path   string
		logger Logger
	}
)

// New - a database instance
func New(dbname string, options *Options) (db DB, err error) {
	opts := Options{}
	if options != nil {
		opts = *options
	}
	if opts.Logger == nil {
		opts.Logger = zrl.New()
	}
	// initiating db
	dbpath := filepath.Join(dbname)
	_, err = getOrCreateDir(dbpath)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return &_db{path: dbpath, logger: opts.Logger}, nil
}

// Collection returns the collection or table
func (db *_db) Collection(name string) (c Collection, err error) {
	collection := filepath.Join(db.path, name)
	dir, err := getOrCreateDir(collection)
	if err != nil {
		db.logger.Error("unable to create db directory", zap.Error(err))
		return nil, err
	}
	if !dir.IsDir() {
		db.logger.Error("not a db directory")
		return nil, fmt.Errorf("not a directory")
	}
	return &_collection{name: name, path: collection, logger: db.logger}, nil
}

// GetAll - returns all records
func (c *_collection) GetAll() (data [][]byte) {
	records, err := os.ReadDir(c.path)
	if err != nil {
		c.logger.Error("no data available")
		return
	}
	for _, r := range records {
		if !r.IsDir() {
			fPath := filepath.Join(c.path, r.Name())
			record, err := os.ReadFile(fPath)
			if err != nil {
				c.logger.Error("unable to read the data file", zap.String("path", fPath))
				continue
			}
			data = append(data, record)
		}
	}
	return
}

// Get help to retrive key based record
func (c *_collection) Get(key string) (data []byte, err error) {
	record := key + _ext
	fPath := filepath.Join(c.path, record)
	r, err := os.Stat(fPath)
	if err != nil {
		c.logger.Error("no data record available", zap.Error(err))
		return
	}
	if r.IsDir() {
		c.logger.Warn("invalid record")
		return nil, fmt.Errorf("invalid record key")
	}
	data, err = os.ReadFile(fPath)
	if err != nil {
		c.logger.Error("unable to read the record", zap.Error(err))
		return
	}
	return
}

// Insert - helps to save data into model dir
func (c *_collection) Create(key string, data []byte) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := key + _ext
	err = os.WriteFile(filepath.Join(c.path, record), data, os.ModePerm)
	if err != nil {
		c.logger.Error("unable to create record", zap.Error(err))
	}
	return
}

// Delete - helps to delete model dir record
func (c *_collection) Delete(key string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := key + _ext
	err = os.Remove(filepath.Join(c.path, record))
	if err != nil {
		c.logger.Error("unable to delete record", zap.Error(err))
	}
	return
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
