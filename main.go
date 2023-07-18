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
		return "", "", "", errors.New("query format:\n\tget [key]\n\t put [key] [value]")
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

	// this is a gross hack, idk why writing a '\n' directly to a byte buffer causes 
	// go to append the '\n' + four null characters
	newline := make([]byte, 1)
	newline[0] = '\n'
	binary.Write(buf, binary.LittleEndian, newline)


	// calc the crc32 hash of the buffer
	hash := crc32.ChecksumIEEE(buf.Bytes())
	binary.Write(hashBuf, binary.LittleEndian, hash)
	hashBuf.Write(buf.Bytes())

	fmt.Println(hashBuf.Bytes())

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	_, err = f.Write(hashBuf.Bytes())
	check(err)

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
	kd_entry, ok := keydir[key]
	if !ok {
		fmt.Println("[ERROR] Key not found")
		return
	}
	file, err := os.Open(kd_entry.file_id)
	check(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// (bytes)   		    	4								4								4									4									n							m					
	// (buffer)   | ---- crc --- | --- tstamp --- | --- keylen --- | --- val_len --- | ---- key --- | --- val --- | 

	res := ""
	latest_tstamp := uint32(0)
	for scanner.Scan() {
		// TODO: crc validation
		// crc := scanner.Bytes()[:4]
		tstamp := binary.LittleEndian.Uint32(scanner.Bytes()[4:8])
		key_len := binary.LittleEndian.Uint32(scanner.Bytes()[8:12])
		val_len := binary.LittleEndian.Uint32(scanner.Bytes()[12:16])
		disc_key := string(scanner.Bytes()[16:16+key_len])
		disc_val := string(scanner.Bytes()[16+key_len:16+key_len+val_len])

		if disc_key == key && tstamp > latest_tstamp {
			latest_tstamp = tstamp
			res = disc_val
		}
	}
	if latest_tstamp == uint32(0) {
		fmt.Println("[ERROR] No key found")
	} else {
		fmt.Println(res)
	}
}

func main() {
	os.Remove("datastore/f2")
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print(">> ")
		text, _ := reader.ReadString('\n')
		// strip and then split 'put key "value"' into array
		op, k, v, err := splitInput(text)
		
		if err != nil {
			fmt.Println(err)
		}
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
