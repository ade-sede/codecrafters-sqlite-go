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
		if len(os.Args) <= 2 {
			log.Fatal("No command specified")
		}

		sqlQuery := os.Args[2]
		stmt, err := sqlparser.Parse(sqlQuery)

		if err != nil {
			log.Fatal(err)
		}

		switch stmt := stmt.(type) {
		case *sqlparser.Select:
			fromExpr := sqlparser.GetTableName(stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName))

			if fromExpr.IsEmpty() {
				log.Fatal("No from table specified in select query")
			}

			tableName := fromExpr.String()
			var tableRootPageNumber int

			for _, records := range allSchemaRecords {
				if string(records[2].payload) == tableName {
					tableRootPageNumber = int(records[3].payload[0])
				}
			}

			tableRootPage, err := database.getPage(tableRootPageNumber - 1)

			if err != nil {
				log.Fatal(err)
			}

			tableCells, err := tableRootPage.cells()

			if err != nil {
				log.Fatal(err)
			}

			allTablePageRecords := make([][]*Record, 0)

			for _, cell := range tableCells {
				records, err := decodePayload(cell.payloadHeader, cell.payloadBody)

				if err != nil {
					log.Fatal(err)
				}

				allTablePageRecords = append(allTablePageRecords, records)
			}

			for _, selectExpr := range stmt.SelectExprs {
				switch selectExpr := selectExpr.(type) {
				case *sqlparser.StarExpr:
					fmt.Println("StarExpr")
				case *sqlparser.AliasedExpr:
					if funcExpr, ok := selectExpr.Expr.(*sqlparser.FuncExpr); ok {
						funcName := funcExpr.Name.String()

						if strings.ToUpper(funcName) == "COUNT" {
							if _, ok := funcExpr.Exprs[0].(*sqlparser.StarExpr); ok {
								fmt.Println(len(allTablePageRecords))
							}
						} else {
							fmt.Println("Unsupported function", funcName)
						}
					}
				default:
					fmt.Println("Neither StarExpr nor AliasedExpr")
				}
			}

		default:
			fmt.Println("Unsupported query", sqlQuery)
			os.Exit(1)
		}

		os.Exit(0)
	}
}
