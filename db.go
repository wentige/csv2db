package main

import (
	"fmt"
	"strings"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	//"github.com/kr/pretty"
)

func openDatabase(cfg Config) *sql.DB {
	host := cfg.Database.Host
	port := cfg.Database.Port
	username := cfg.Database.Username
	password := cfg.Database.Password
	name := cfg.Database.Name

	if port == "" {
		port = "3306"
	}

	var dsn string
	if password == "" {
		dsn = fmt.Sprintf("%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true", username, host, port, name)
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true", username, password, host, port, name)
	}
	//println(dsn)
	//return nil

	db, err := sql.Open("mysql", dsn)
	check(err)

	err = db.Ping()
	check(err)

	return db
}

func insertSql(table string, columns []string, data []map[string]string) string {

	columnStr := strings.Join(columns, "`, `")

	updateList := make([]string, len(columns))
	for i, col := range columns {
		updateList[i] = fmt.Sprintf("`%s`=VALUES(`%s`)", col, col)
	}
	updateStr := strings.Join(updateList, ",\n")

	valueList := make([]string, 0)
	for _, row := range data {
		valueRow := make([]string, 0)
		//for _, val := range row { // WRONG
		for _, col := range columns {
			valueRow = append(valueRow, "'"+row[col]+"'")
		}
		valueList = append(valueList, "("+strings.Join(valueRow, ", ")+")")
	}
	valueStr := strings.Join(valueList, ",\n")

	//return "INSERT INTO `" + table + "` (" + columnStr + ") VALUES\n" + valueStr + updateStr;
	return fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES\n%s\nON DUPLICATE KEY UPDATE\n%s",
		table, columnStr, valueStr, updateStr)
}
