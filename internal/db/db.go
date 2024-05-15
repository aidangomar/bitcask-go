package db

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"hash/crc32"
	"os"
	"strings"
	"time"
)

var datafile = "datastore/" + uuid.New().String()
var keydir = make(map[string]kd_entry)
var badQuery = errors.New("[ERROR] query format:\n  get [key]\n  put [key] \"[value]\"")

// threshold for file size
var sizeThreshold = 2048

type kd_entry struct {
	file_id  string
	value_sz uint32
	tstamp   uint32
}

type file_entry struct {
	crc     uint32
	tstamp  uint32
	key_len uint32
	val_len uint32
	key     string
	val     string
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func SplitInput(input string) (string, string, string, error) {
	parts := strings.Split(input, " ")
	if len(parts) < 2 {
		return "", "", "", badQuery
	}

	op := parts[0]
	key := parts[1]

	if op == "put" && len(parts) != 3 {
		return "", "", "", badQuery
	}

	value := strings.Join(parts[2:], " ")
	value = strings.ReplaceAll(value, "\"", "")
	value = strings.ReplaceAll(value, "\n", "")
	key = strings.ReplaceAll(key, "\n", "")

	return op, key, value, nil
}

/*
Writes buffer to current datafile
*/
func dbWrite(buf *bytes.Buffer) {
	f, err := os.OpenFile(datafile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	check(err)
	defer f.Close()

	// check if datafile exceeds size threshold
	fi, err := os.Stat(datafile)
	check(err)

	// write new datafile if so
	if fi.Size() > int64(sizeThreshold) {
		datafile = uuid.New().String()
	}

	// write to file
	_, err = f.Write(buf.Bytes())
	check(err)
}


/*
Writes kv pair to database and commits entry to keydir
*/
func HandlePut(key string, value string) (err error) {

	buf := new(bytes.Buffer)
	hashBuf := new(bytes.Buffer)
	t := time.Now().Unix()

	binary.Write(buf, binary.LittleEndian, uint32(t))
	binary.Write(buf, binary.LittleEndian, uint32(len(key)))
	binary.Write(buf, binary.LittleEndian, uint32(len(value)))
	binary.Write(buf, binary.LittleEndian, []byte(key))
	binary.Write(buf, binary.LittleEndian, []byte(value))

	newline := make([]byte, 1)
	newline[0] = '\n'
	binary.Write(buf, binary.LittleEndian, newline)

	// calc the crc32 hash of the buffer
	hash := crc32.ChecksumIEEE(buf.Bytes())
	binary.Write(hashBuf, binary.LittleEndian, hash)
	hashBuf.Write(buf.Bytes())

	// write to fs
	dbWrite(hashBuf)

	// write to the keydir
	keydir[key] = kd_entry{
		file_id:  datafile,
		value_sz: uint32(len(value)),
		tstamp:   uint32(t),
	}
	return err
}

/*
Reads file entry from scanner
*/
func readFileEntry(scanner *bufio.Scanner) file_entry {
	key_len := binary.LittleEndian.Uint32(scanner.Bytes()[8:12])
	val_len := binary.LittleEndian.Uint32(scanner.Bytes()[12:16])
	return file_entry{
		crc:     binary.LittleEndian.Uint32(scanner.Bytes()[:4]),
		tstamp:  binary.LittleEndian.Uint32(scanner.Bytes()[4:8]),
		key_len: key_len,
		val_len: val_len,
		key:     string(scanner.Bytes()[16 : 16+key_len]),
		val:     string(scanner.Bytes()[16+key_len : 16+key_len+val_len]),
	}
}

/*
Finds the latest entry for a key and prints the value to stdout
*/
func HandleGet(key string) {
	kd_entry, exists := keydir[key]
	if !exists {
		fmt.Println("[ERROR] Key not found")
		return
	}
	file, err := os.Open(kd_entry.file_id)
	check(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var val string
	latest_tstamp := uint32(0)

	for scanner.Scan() {
		entry := readFileEntry(scanner)
		if entry.key == key && entry.tstamp > latest_tstamp {
			latest_tstamp = entry.tstamp
			val = entry.val
		}
	}
	if latest_tstamp == 0 {
		fmt.Println("[ERROR] No key found")
	} else {
		fmt.Println(val)
	}
}


func entryToBuffer(entry file_entry) *bytes.Buffer {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, entry.crc)
	binary.Write(buf, binary.LittleEndian, entry.tstamp)
	binary.Write(buf, binary.LittleEndian, entry.key_len)
	binary.Write(buf, binary.LittleEndian, entry.val_len)
	binary.Write(buf, binary.LittleEndian, []byte(entry.key))
	binary.Write(buf, binary.LittleEndian, []byte(entry.val))
	return buf
}


func Merge(file_id string) {
	file_id = datafile
	f, err := os.OpenFile(file_id, os.O_RDWR, 0600)
	check(err)
	defer f.Close()

	// count occurences of each key
	key_count := make(map[string]uint)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		entry := readFileEntry(scanner)
		key_count[entry.key]++
	}

	// create new datafile to copy over merged entries
	tmp, err := os.OpenFile(uuid.New().String(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	check(err)
	defer f.Close()

	// write entries to new datafile
	scanner = bufio.NewScanner(f)
	for scanner.Scan() {
		entry := readFileEntry(scanner)
		if key_count[entry.key] > 1 {
			key_count[entry.key]--
			continue
		}
		buf := entryToBuffer(entry)
		tmp.Write(buf.Bytes())
	}
	
	// replace old datafile with new datafile
	os.Remove(file_id)
	os.Rename(tmp.Name(), file_id)

}

