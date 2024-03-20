package main

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"log"
	"os"
	"sort"
	"strings"
)

func getSchemaTableRecords(database *Database) ([][]*Record, error) {
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

	allSchemaRecords, err := getSchemaTableRecords(database)

	if err != nil {
		log.Fatal(err)
	}

	defer database.Close()

	switch command {
	case ".dbinfo":
		fmt.Printf("database page size: %v\n", database.PageSize)

		if err != nil {
			log.Fatal(err)
		}

		tableNames := make([]string, 0)

		for _, records := range allSchemaRecords {
			schemaType := string(records[0].payload)
			schemaName := string(records[2].payload)

			if schemaType == "table" && !strings.HasPrefix(schemaName, "sqlite") {
				tableNames = append(tableNames, schemaName)
			}
		}

		fmt.Printf("number of tables: %v\n", len(tableNames))

	case ".tables":
		if err != nil {
			log.Fatal(err)
		}

		tableNames := make([]string, 0)

		for _, records := range allSchemaRecords {
			schemaType := string(records[0].payload)
			schemaName := string(records[2].payload)

			if schemaType == "table" && !strings.HasPrefix(schemaName, "sqlite") {
				tableNames = append(tableNames, schemaName)
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
