package simplejsondb

import (
	"os"
	"path/filepath"
	"strings"
)

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

// Create - helps to save data into model dir
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
