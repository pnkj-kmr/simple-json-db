package test_test

import (
	"fmt"
	"testing"

	"github.com/pnkj-kmr/simple-json-db"
)

func TestCollection_GetAll(t *testing.T) {
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
	table := randName(5)
	c, err := db.Collection(table)
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path, table)
	if err != nil {
		t.Error(err)
	}

	data := c.GetAll()
	if len(data) != 0 {
		t.Error("zero count expected")
	}

	var data2 []byte
	data2 = append(data2, 99)
	err = c.Create("ip-dummy", data2)
	if err != nil {
		t.Error("Test failed - ", err)
	}
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json")

	data = c.GetAll()
	if len(data) != 1 {
		t.Error("zero count expected")
	}
}

func TestCollection_Get2(t *testing.T) {
	path := randName(5)
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path)

	db, err := simplejsondb.New(path, nil)
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
	fmt.Println(path, table, err)
	if err == nil {
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
	}(path, table, "ip-dummy.json")

	_, err = c.Get("ip-dummy")
	fmt.Println(path, table, err)
	if err != nil {
		t.Error("Test failed - ", err)
	}
}

func TestCollection_Delete(t *testing.T) {
	path := randName(6)
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path)

	db, err := simplejsondb.New(path, nil)
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

	err = c.Delete("test_dummp")
	if err == nil {
		// We expect an error because the file doesn't exist
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

func TestCollection_Len(t *testing.T) {
	path := randName(6)
	defer func(dir ...string) {
		if err := remove(dir...); err != nil {
			t.Error(err)
		}
	}(path)

	db, err := simplejsondb.New(path, nil)
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

	total := c.Len()
	if total != 0 {
		t.Error("record should zero")
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
	}(path, table, "ip-dummy.json")

	total = c.Len()
	if total != 1 {
		t.Error("record should 1")
	}
}
