package simplejsondb_test

import (
	"os"
	"testing"

	simplejsondb "github.com/pnkj-kmr/simple-json-db"
)

func TestNew(t *testing.T) {
	path := "database1"
	_, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestNewCollection(t *testing.T) {
	path := "database1"
	db, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}
	_, err = db.Collection("collection1")
	if err != nil {
		t.Error(err)
	}
}

func TestGetAll(t *testing.T) {
	path := "database1"
	db, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}
	c, err := db.Collection("collection1")
	if err != nil {
		t.Error(err)
	}

	_ = c.GetAll()
}

func TestGet(t *testing.T) {
	path := "database1"
	db, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}
	c, err := db.Collection("collection1")
	if err != nil {
		t.Error(err)
	}
	_, err = c.Get("ip-dummy")
	if os.IsExist(err) {
		t.Error("Test failed - ", err)
	}
}

func TestInsert(t *testing.T) {
	path := "database1"
	db, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}
	c, err := db.Collection("collection1")
	if err != nil {
		t.Error(err)
	}
	var data []byte
	data = append(data, 99)
	err = c.Create("ip-dummy", data)
	if err != nil {
		t.Error("Test failed - ", err)
	}
}

func TestGZipInsert(t *testing.T) {
	path := "database1"
	db, err := simplejsondb.New(path, &simplejsondb.Options{UseGzip: true})
	if err != nil {
		t.Error(err)
	}
	c, err := db.Collection("collection1")
	if err != nil {
		t.Error(err)
	}
	var data []byte
	data = append(data, 99)
	err = c.Create("ip-dummy", data)
	if err != nil {
		t.Error("Test failed - ", err)
	}
}

func TestGet2(t *testing.T) {
	path := "database1"
	db, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}
	c, err := db.Collection("collection1")
	if err != nil {
		t.Error(err)
	}
	_, err = c.Get("ip-dummy")
	if os.IsNotExist(err) {
		t.Error("Test failed - ", err)
	}
}

func TestGetGZip(t *testing.T) {
	path := "database1"
	db, err := simplejsondb.New(path, &simplejsondb.Options{UseGzip: true})
	if err != nil {
		t.Error(err)
	}
	c, err := db.Collection("collection1")
	if err != nil {
		t.Error(err)
	}
	_, err = c.Get("ip-dummy")
	if os.IsNotExist(err) {
		t.Error("Test failed - ", err)
	}
}

func TestDelete(t *testing.T) {
	path := "database1"
	db, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}
	c, err := db.Collection("collection1")
	if err != nil {
		t.Error(err)
	}
	err = c.Delete("test_dummp")
	if os.IsExist(err) {
		t.Error("Test failed - ", err)
	}

	err = c.Delete("test_dummp")
	if err == nil {
		t.Error("Test failed", err)
	}

	_, err = c.Get("test_dummp")
	if err == nil {
		t.Error("Test failed", err)
	}
}
