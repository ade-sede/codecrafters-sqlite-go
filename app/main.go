package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/xwb1989/sqlparser"
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

func aliasedSelectExpr(allSchemaRecords [][]*Record, allTablePageRecords [][]*Record, selectExpr *sqlparser.AliasedExpr, tableName string) ([]string, error) {
	data := make([]string, 0)

	switch expr := selectExpr.Expr.(type) {
	case *sqlparser.FuncExpr:
		funcName := expr.Name.String()

		if strings.ToUpper(funcName) != "COUNT" {
			return nil, errors.New("Unsupported function")
		}

		if _, ok := expr.Exprs[0].(*sqlparser.StarExpr); ok {
			data = append(data, fmt.Sprintf("%v", len(allTablePageRecords)))

			return data, nil
		}
	case *sqlparser.ColName:
		columnName := expr.Name.String()
		var tableSchema string

		for _, records := range allSchemaRecords {
			if string(records[2].payload) == tableName {
				tableSchema = string(records[4].payload)
			}
		}

		tableSchema = strings.ReplaceAll(tableSchema, "autoincrement", "")
		createStmt, err := sqlparser.Parse(tableSchema)

		if err != nil {
			return nil, err
		}

		indexOfColumnToRead := -1

		switch createStmt := createStmt.(type) {
		case *sqlparser.DDL:
			for index, columnDef := range createStmt.TableSpec.Columns {
				if columnDef.Name.String() == columnName {
					indexOfColumnToRead = index
					break
				}
			}

			if indexOfColumnToRead == -1 {
				return nil, errors.New("Column not found")
			}

			for _, records := range allTablePageRecords {
				data = append(data, string(records[indexOfColumnToRead].payload))
			}

			return data, nil
		default:
			return nil, errors.New("Malformed create statement")
		}
	}

	return nil, errors.New("Unsupported select expression")
}

func getTableData(database *Database, tableRootPageNumber int) ([][]*Record, error) {
	tableRootPage, err := database.getPage(tableRootPageNumber - 1)

	if err != nil {
		return nil, err
	}

	tableCells, err := tableRootPage.cells()

	if err != nil {
		return nil, err
	}

	allTablePageRecords := make([][]*Record, 0)

	for _, cell := range tableCells {
		records, err := decodePayload(cell.payloadHeader, cell.payloadBody)

		if err != nil {
			return nil, err
		}

		allTablePageRecords = append(allTablePageRecords, records)
	}

	return allTablePageRecords, nil
}

func selectExpr(database *Database, allSchemaRecords [][]*Record, stmt *sqlparser.Select) {
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

	allTablePageRecords, err := getTableData(database, tableRootPageNumber)

	if err != nil {
		log.Fatal(err)
	}

	for _, selectExpr := range stmt.SelectExprs {
		switch selectExpr := selectExpr.(type) {
		case *sqlparser.StarExpr:
			fmt.Println("StarExpr")
		case *sqlparser.AliasedExpr:
			columnValues, err := aliasedSelectExpr(allSchemaRecords, allTablePageRecords, selectExpr, tableName)

			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(strings.Join(columnValues, "\n"))
		default:
			fmt.Println("Neither StarExpr nor AliasedExpr")
		}
	}

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
			selectExpr(database, allSchemaRecords, stmt)
		default:
			fmt.Println("Unsupported query", sqlQuery)
			os.Exit(1)
		}

		os.Exit(0)
	}
}
