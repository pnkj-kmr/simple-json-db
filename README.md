# A simple JSON database

Provides a quick file based database which helps to store the json file based data along with key based search.

_sample structure will be generated by below code_

```
database1

    collection1
        key1.json  `{"key": "val", ...}`
        ...

    collection2
        key1.json   `{"key": "val", ...}`
        ...


```

## HOW TO USE

---

```
package main

import (
	"fmt"

	simplejsondb "github.com/pnkj-kmr/simple-json-db"
)

func main() {
	// db instance
	db, err := simplejsondb.New("database1", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("database created")

	// collection1 creation
	t, err := db.Collection("collection1")
	if err != nil {
		fmt.Println("table err", err)
		return
	}
	fmt.Println("collection1 created")

	// collection1 - inserting a record
	data := "{\"key\": 123}"
	err = t.Create("key1", []byte(data))
	if err != nil {
		fmt.Println(err)
		return
	}
	// collection1 - inserting  a record 2
	err = t.Create("key2", []byte(data))
	if err != nil {
		fmt.Println(err)
		return
	}

	// collection2 creation
	t2, err := db.Collection("collection2")
	if err != nil {
		fmt.Println("table err", err)
		return
	}
	fmt.Println("collection2 created")

	// collection2 - inserting a record
	data2 := "{\"key\": 124}"
	err = t2.Create("key1", []byte(data2))
	if err != nil {
		fmt.Println(err)
		return
	}

	// fetching all records from collection1
	records := t.GetAll()
	for _, r := range records {
		fmt.Println(string(r))
	}

	// getting one record from collection1
	_, err = t.Get("key3")
	if err != nil {
		fmt.Println("record get-", err)
	}
	record, err := t.Get("key2")
	if err != nil {
		fmt.Println("record get-", err)
	}
	fmt.Println("record -- ", string(record))

	// deleting record from collection1
	err = t.Delete("key2")
	if err != nil {
		fmt.Println("record delete--", err)
	}

	// fecthing all records from collection1
	fmt.Println("after delete of record with key2..")
	records = t.GetAll()
	for _, r := range records {
		fmt.Println(string(r))
	}
}
```

## DESCRIPTION

---

A simple JSON database helps to store the json file based data into your current working directory, you can define N number of databases and One database can contains N number of collections(tables) and One collection can contains N number of records(entries).

To install:

```
go install github.com/pnkj-kmr/simple-json-db
```

## FAQ?

**_Why, we need simple-json-db?_**

_This database helps to maintain the small dataset to keep into files by avoiding extra affects to maintain a seperate database along the connection libraries_

_:)_
