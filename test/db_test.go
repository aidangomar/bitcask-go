package bitcask

import (
	"testing"
	"bitcask-go/internal/bitcask"
	"strconv"
)

func TestDB(t *testing.T) {
	db, err := bitcask.Open("datastore", bitcask.READ|bitcask.WRITE|bitcask.CREATE)
	if err != nil {
		panic(err)
	}
	
	err = db.Put("key1", "val1")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	err = db.Put("something", "else")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	err = db.Delete("key1")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	err = db.Put("some", "new")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	// test key1 to equal val1
	key, err := db.Get("key1")
	if err == nil {
		t.Fatalf("Error: %v", err)
	}
	if key == "val1" {
		t.Fatalf("Expected val1, got %v", key)
	}
	key, err = db.Get("something")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	if key != "else" {
		t.Fatalf("Expected else, got %v", key)
	}

	key, err = db.Get("some")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	if key != "new" {
		t.Fatalf("Expected new, got %v", key)
	}


	for i := 0; i < 10000; i++ {
		// Put string i as key and value
		err = db.Put(strconv.Itoa(i), strconv.Itoa(i))
		if err != nil {
			panic(err)
		}
		if i % 2 == 0 {
			err = db.Delete(strconv.Itoa(i))
			if err != nil {
				panic(err)
			}
		}
	}

	err = db.Merge()
	if err != nil {
		panic(err)
	}
}
