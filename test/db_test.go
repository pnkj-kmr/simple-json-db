package test_test

import (
	"testing"

	"github.com/pnkj-kmr/simple-json-db"
)

func TestDB(t *testing.T) {
	path := randName(4)
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path)

	_, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestDB_Collection(t *testing.T) {
	path := randName(4)
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path)

	db, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}
	c := randName(6)
	_, err = db.Collection(c)
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path, c)

	if err != nil {
		t.Error(err)
	}
}
