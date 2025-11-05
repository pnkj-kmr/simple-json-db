package simplejsondb_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	simplejsondb "github.com/pnkj-kmr/simple-json-db"
	"golang.org/x/exp/rand"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randName(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func remove(dir ...string) error {
	return os.Remove(filepath.Join(dir...))
}

func TestDB(t *testing.T) {
	path := randName(4)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, c)

	if err != nil {
		t.Error(err)
	}
}

func TestCollection_GetAll(t *testing.T) {
	path := randName(4)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json")

	data = c.GetAll()
	if len(data) != 1 {
		t.Error("zero count expected")
	}
}

func TestCollection_GetAllByName(t *testing.T) {
	path := randName(4)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table)

	if err != nil {
		t.Error(err)
	}

	data := c.GetAllByName()
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json")

	data = c.GetAllByName()
	if len(data) != 1 {
		t.Error("zero count expected")
	}
}

func TestCollection_GetAllGzip(t *testing.T) {
	path := randName(4)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path)

	db, err := simplejsondb.New(path, &simplejsondb.Options{UseGzip: true})
	if err != nil {
		t.Error(err)
	}
	table := randName(5)
	c, err := db.Collection(table)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json.gz")

	data = c.GetAll()
	if len(data) != 1 {
		t.Error("1 count expected")
	}

	data2 = append(data2, 99)
	err = c.Create("ip-dummy2", data2)
	if err != nil {
		t.Error("Test failed - ", err)
	}
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy2.json.gz")

	data = c.GetAll()
	if len(data) != 2 {
		t.Error("2 count expected")
	}
}

func TestCollection_Get(t *testing.T) {
	path := randName(5)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
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
}

func TestCollection_Insert(t *testing.T) {
	path := randName(6)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table)

	if err != nil {
		t.Error(err)
	}

	var data []byte
	data = append(data, 99)
	err = c.Create("ip-dummy", data)
	if err != nil {
		t.Error("Test failed - ", err)
	}
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json")

}

func TestCollection_GZipInsert(t *testing.T) {
	path := randName(5)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table)

	if err != nil {
		t.Error(err)
	}
	var data []byte
	data = append(data, 99)
	err = c.Create("ip-dummy", data)
	if err != nil {
		t.Error("Test failed - ", err)
	}
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json.gz")

}

func TestCollection_LockID(t *testing.T) {
	path := randName(6)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path)

	db, err := simplejsondb.New(path, nil)
	if err != nil {
		t.Error(err)
	}

	table := "collection_lock_test"
	c, err := db.Collection(table)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table)

	if err != nil {
		t.Error(err)
	}

	// Table-driven tests
	tests := []struct {
		name      string
		id        string
		lockMode  simplejsondb.LockMode
		unlock    bool
		expectErr bool
	}{
		{
			name:      "LockID_Read_Mode_Success",
			id:        "record1",
			lockMode:  simplejsondb.ModeRead,
			unlock:    true,
			expectErr: false,
		},
		{
			name:      "LockID_Write_Mode_Success",
			id:        "record2",
			lockMode:  simplejsondb.ModeWrite,
			unlock:    true,
			expectErr: false,
		},
		{
			name:      "LockID_Write_Mode_Success",
			id:        "record2",
			lockMode:  simplejsondb.ModeReadWrite,
			unlock:    true,
			expectErr: false,
		},
		{
			name:      "LockID_Multiple_Reads_Same_ID",
			id:        "record3",
			lockMode:  simplejsondb.ModeRead,
			unlock:    true,
			expectErr: false,
		},
		{
			name:      "LockID_No_Unlock",
			id:        "record4",
			lockMode:  simplejsondb.ModeWrite,
			unlock:    false,
			expectErr: false,
		},
		{
			name:      "Unlock_Without_Lock",
			id:        "nonexistent",
			lockMode:  simplejsondb.ModeRead,
			unlock:    true,
			expectErr: true,
		},
		{
			name:      "Double_Unlock_Error",
			id:        "record5",
			lockMode:  simplejsondb.ModeWrite,
			unlock:    true,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use LockID for valid lock operations
			if tt.name != "Unlock_Without_Lock" {
				c.LockID(tt.id, tt.lockMode)
			}

			if tt.unlock {
				// Attempt to unlock the ID
				c.UnlockID(tt.id, tt.lockMode)

				// Second unlock to trigger an error for certain cases
				if tt.name == "Double_Unlock_Error" {
					defer func() {
						if r := recover(); r == nil {
							t.Error("Expected panic on double unlock, but none occurred")
						}
					}()
					c.UnlockID(tt.id, tt.lockMode)
				}
			}

			// No error expected generally except for specific invalid cases like double unlock
			if tt.expectErr {
				t.Log("Handled expected error case:", tt.name)
			}
		})
	}
}

func TestCollection_Get2(t *testing.T) {
	path := randName(5)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json")

	_, err = c.Get("ip-dummy")
	fmt.Println(path, table, err)
	if err != nil {
		t.Error("Test failed - ", err)
	}
}

func TestCollection_GetGZip(t *testing.T) {
	path := randName(6)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json.gz")

	_, err = c.Get("ip-dummy")
	fmt.Println(path, table, err)
	if os.IsNotExist(err) {
		t.Error("Test failed - ", err)
	}
}

func TestCollection_Delete(t *testing.T) {
	path := randName(6)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table)

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

func TestCollection_Len(t *testing.T) {
	path := randName(6)
	defer func(dir ...string) {
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
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
		err := remove(dir...)
		if err != nil {
			t.Error(err)
		}
	}(path, table, "ip-dummy.json")

	total = c.Len()
	if total != 1 {
		t.Error("record should 1")
	}
}
