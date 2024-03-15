package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		database, err := NewDBHandler(databaseFilePath)
		defer database.Close()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("database page size: %v\n", database.PageSize)

		rootPage, err := database.getPage(0)

		if err != nil {
			log.Fatal(err)
		}

		cells, err := rootPage.cells()

		if err != nil {
			log.Fatal(err)
		}

		tableNames := make([]string, 0)

		for _, cell := range cells {
			records, err := decodePayload(cell.payloadHeader, cell.payloadBody)

			if err != nil {
				log.Fatal(err)
			}

			t := string(records[0].payload)
			v := string(records[2].payload)

			if t == "table" && !strings.HasPrefix(v, "sqlite") {
				tableNames = append(tableNames, v)
			}

		}

		fmt.Printf("number of tables: %v\n", len(tableNames))

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
