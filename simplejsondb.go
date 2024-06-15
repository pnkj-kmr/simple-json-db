package simplejsondb

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	zrl "github.com/pnkj-kmr/zap-rotate-logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Ext string = ".json"
var GZipExt string = ".json.gz"

type (
	// Options - extra configuration
	Options struct {
		UseGzip bool
		Logger
	}

	CreateOptions struct {
		UseGzip bool
	}

	_db struct {
		useGzip bool
		path    string
		logger  Logger
	}

	_collection struct {
		useGzip bool
		mu      sync.Mutex
		name    string
		path    string
		logger  Logger
	}
)

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
		Create(string, []byte, ...CreateOptions) error
		Delete(string) error
	}
	// DB - a database
	DB interface {
		Collection(string) (Collection, error)
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
	return &_db{path: dbpath, logger: opts.Logger, useGzip: opts.UseGzip}, nil
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
	return &_collection{name: name, path: collection, logger: db.logger, useGzip: db.useGzip}, nil
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

			if strings.LastIndex(r.Name(), GZipExt) > 0 {
				record, err = UnGzip(record)
				if err != nil {
					c.logger.Error("unable to unzip the data file", zap.String("path", fPath))
				}
			}

			data = append(data, record)
		}
	}
	return
}

// Get help to retrive key based record
func (c *_collection) Get(key string) (data []byte, err error) {
	filename, err, isGzip := c.getPathIfExist(key, err)
	data, err = os.ReadFile(filename)
	if err != nil {
		c.logger.Error("unable to read the record", zap.Error(err))
	}

	if isGzip {
		data, err = UnGzip(data)
		if err != nil {
			c.logger.Error("unable to unzip the data file", zap.String("path", filename))
		}
	}

	return
}

// Insert - helps to save data into model dir
func (c *_collection) Create(key string, data []byte, options ...CreateOptions) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var useGzip bool = c.useGzip
	if !c.useGzip {
		if options != nil && options[0].UseGzip {
			useGzip = options[0].UseGzip
		}
	}
	filename := c.getFullPath(key, c.useGzip)
	if err != nil {
		c.logger.Error("unable to create record", zap.Error(err))
	}

	if useGzip {
		data, err = c.Gzip(data)
	}
	err = os.WriteFile(filename, data, os.ModePerm)
	if err != nil {
		c.logger.Error("unable to create record", zap.Error(err))
	}
	return
}

// Delete - helps to delete model dir record
func (c *_collection) Delete(key string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	filename, err, _ := c.getPathIfExist(key, err)
	if err != nil {
		return err
	}

	err = os.Remove(filename)
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

func (c *_collection) getFullPath(key string, isGzip bool) string {
	var record string
	if isGzip {
		record = key + GZipExt
	} else {
		record = key + Ext
	}
	filename := filepath.Join(c.path, record)

	return filename
}

func (c *_collection) getPathIfExist(key string, err error) (string, error, bool) {
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

func (c *_collection) isExist(filename string, err error) (bool, error) {
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

func (c *_collection) Gzip(data []byte) (result []byte, err error) {
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
