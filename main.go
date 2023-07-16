package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"time"
)

var filePath = "datastore/f2"
var keydir = make(map[string]kd_entry)

type kd_entry struct {
	file_id   string
	value_sz  uint32
	tstamp    uint32
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func splitInput(input string) (string, string, string, error) {
	parts := strings.Split(input, " ")
	if len(parts) < 2 {
		return "", "", "", errors.New("query format: \n\tget [key] \n\t put [key] [value]")
	}

	op := parts[0]
	key := parts[1]

	value := strings.Join(parts[2:], " ")
	value = strings.ReplaceAll(value, "\"", "")
	value = strings.ReplaceAll(value, "\n", "")
	key = strings.ReplaceAll(key, "\n", "")

	return op, key, value, nil
}

func handlePut(key string, value string) (err error) {
	buf := new(bytes.Buffer)
	hashBuf := new(bytes.Buffer)
	t := time.Now().Unix()

	binary.Write(buf, binary.LittleEndian, uint32(t))
	binary.Write(buf, binary.LittleEndian, uint32(len(key)))
	binary.Write(buf, binary.LittleEndian, uint32(len(value)))
	binary.Write(buf, binary.LittleEndian, []byte(key))
	binary.Write(buf, binary.LittleEndian, []byte(value))

	// calc the crc32 hash of the buffer
	hash := crc32.ChecksumIEEE(buf.Bytes())
	binary.Write(hashBuf, binary.LittleEndian, hash)

	// concat the buffers
	hashBuf.Write(buf.Bytes())

	err = os.WriteFile(filePath, hashBuf.Bytes(), 0644)

	// write to the keydir
	entry := kd_entry {
		file_id:   filePath,
		value_sz:  uint32(len(value)),
		tstamp:    uint32(t),
	}
	
	// add the entry to the keydir
	keydir[key] = entry

	return err
}

func handleGet(key string) {
	kd_entry := keydir[key]
	file, err := os.Open(kd_entry.file_id)
	check(err)
	defer file.Close()

	scanner := bufio.NewScanner(strings.NewReader(filePath))
	// highest_tstamp := uint32(0)
	// first := true
	for scanner.Scan() {
				// if first == true {
				// 	first = false
				// 	continue
				// }
        fmt.Println(scanner.Bytes())



		// // get the timestamp from the line
		// tstamp := scanner.Bytes()[:4]
		// // convert the tstamp to uint32
		// tstamp_int := binary.LittleEndian.Uint32(tstamp)
		// fmt.Println(tstamp_int)
		// // get the key length from the line
		// key_len := scanner.Bytes()[4:8]
		// // convert the key length to uint32
		// key_len_int := binary.LittleEndian.Uint32(key_len)
		// fmt.Println(key_len_int)
		// // get the key from the line
		// key := scanner.Bytes()[8:8+binary.LittleEndian.Uint32(key_len)]
		// fmt.Println(string(key))


	}
}

func main() {

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print(">> ")
		text, _ := reader.ReadString('\n')
		// strip and then split 'put key "value"' into array
		op, k, v, err := splitInput(text)
		check(err)


		if op == "put" {
			err := handlePut(k, v); 
			if err == nil {
				fmt.Println(":OK")
			} else {
				fmt.Println(":ERROR")
			}
		}

		if op == "get" {
			handleGet(k)
		}

	}

}
