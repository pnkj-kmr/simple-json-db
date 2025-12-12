package simplejsondb

import "errors"

var (
	Ext            string = ".json"
	GZipExt        string = ".json.gz"
	ErrNoDirectory error  = errors.New("not a directory")
)
