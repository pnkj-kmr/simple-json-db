package test_test

import (
	"os"
	"testing"

	"github.com/pnkj-kmr/simple-json-db"
)

func TestCollection_GetGZip(t *testing.T) {
	path := randName(6)
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path)

	db, err := simplejsondb.New(path, &simplejsondb.Options{UseGzip: true})
	if err != nil {
		t.Error(err)
	}
	table := "collection1"
	c, err := db.Collection(table)
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path, table)

	if err != nil {
		t.Error(err)
	}

	_, err = c.Get("ip-dummy")
	if os.IsExist(err) {
		t.Error("Test failed - ", err)
	}

	var data []byte
	data = append(data, 99)
	err = c.Create("ip-dummy", data)
	if err != nil {
		t.Error("Test failed - ", err)
	}
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json.gz")

	_, err = c.Get("ip-dummy")
	if os.IsNotExist(err) {
		t.Error("Test failed - ", err)
	}
}
