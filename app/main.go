package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

func schema_table_records(database *Database) ([][]*Record, error) {
	rootPage, err := database.getPage(0)

	if err != nil {
		return nil, err
	}

	cells, err := rootPage.cells()

	if err != nil {
		return nil, err
	}

	allSchemaRecords := make([][]*Record, 0)

	for _, cell := range cells {
		records, err := decodePayload(cell.payloadHeader, cell.payloadBody)

		if err != nil {
			return nil, err
		}

		allSchemaRecords = append(allSchemaRecords, records)
	}

	return allSchemaRecords, nil
}

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	database, err := NewDBHandler(databaseFilePath)

	if err != nil {
		log.Fatal(err)
	}

	defer database.Close()

	switch command {
	case ".dbinfo":
		fmt.Printf("database page size: %v\n", database.PageSize)

		allSchemaRecords, err := schema_table_records(database)

		if err != nil {
			log.Fatal(err)
		}

		tableNames := make([]string, 0)

		for _, records := range allSchemaRecords {
			t := string(records[0].payload)
			v := string(records[2].payload)

			if t == "table" && !strings.HasPrefix(v, "sqlite") {
				tableNames = append(tableNames, v)
			}
		}

		fmt.Printf("number of tables: %v\n", len(tableNames))

	case ".tables":
		allSchemaRecords, err := schema_table_records(database)

		if err != nil {
			log.Fatal(err)
		}

		tableNames := make([]string, 0)

		for _, records := range allSchemaRecords {
			t := string(records[0].payload)
			v := string(records[2].payload)

			if t == "table" && !strings.HasPrefix(v, "sqlite") {
				tableNames = append(tableNames, v)
			}
		}

		sort.Strings(tableNames)
		allTables := strings.Join(tableNames, " ")

		fmt.Println(allTables)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
