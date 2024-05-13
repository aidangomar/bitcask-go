package cli

import (
	"bitcask-go/internal/db"
	"bufio"
	"fmt"
	"os"
)

func Cli() {
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print(">> ")
		text, _ := reader.ReadString('\n')
		// strip and then split 'put key "value"' into array
		op, k, v, err := db.SplitInput(text)

		if err != nil {
			fmt.Println(err)
		}
		if op == "put" {
			err := db.HandlePut(k, v)
			if err == nil {
				fmt.Println(":OK")
			} else {
				fmt.Println(":ERROR")
			}
		}
		if op == "get" {
			db.HandleGet(k)
		}
	}
}
