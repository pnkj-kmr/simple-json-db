package simplejsondb

import (
	"os"
	"path/filepath"
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
