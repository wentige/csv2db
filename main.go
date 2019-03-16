package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kr/pretty"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type Database struct {
	Host     string `toml:"host"`
	Port     string `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	Name     string `toml:"dbname"`
}

type File struct {
	CsvFile   string            `toml:"csvfile"`
	Delimiter string            `toml:"delimiter"`
	HasHeader bool              `toml:"hasheader"`
	Table     string            `toml:"table"`
	ColMap    map[string]string `toml:"colmap"`
}

// ./config.toml
type Config struct {
	Database Database `toml:"Database"`
	Files    []File   `toml:"Files"`
}

func main() {
	cfg := loadConfig()

	//var db *sql.DB
	db := openDatabase(cfg)
	defer db.Close()

	for _, file := range cfg.Files {
		importFile(file, db)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
		fmt.Println(e)
		pretty.Println(e)
	}
}

func loadConfig() Config {
	data, err := ioutil.ReadFile("config.toml")
	check(err)

	var cfg Config
	toml.Decode(string(data), &cfg)
	check(err)

	//pretty.Println(cfg)

	//fmt.Printf("%+v\n", cfg)
	//fmt.Println(cfg.Files[0].CsvFile)
	//fmt.Println(cfg.Files[0].HasTitle)
	//fmt.Println(cfg.Files[0].Table)

	return cfg
}

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

func importFile(file File, db *sql.DB) {
	filename := file.CsvFile
	table := file.Table
	colMap := file.ColMap

	log.Printf("Importing csv file '%s' into table '%s'\n", filename, table)

	db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", table))

	tabcols := make([]string, len(colMap))
	csvcols := make([]int, len(colMap))

	var i = 0
	for idx, colname := range colMap {
		k, _ := strconv.Atoi(idx)
		tabcols[i] = colname
		csvcols[i] = k - 1 // the index is 1-based
		i++
	}

	//pretty.Println(tabcols)
	//pretty.Println(csvcols)

	records, err := readCsvFile(file)
	if err == nil {
		//pretty.Println(records)

		buf := make([]map[string]string, 0)
		for _, fields := range records {
			row := make(map[string]string)
			for i, colname := range tabcols {
				c := csvcols[i]
				row[colname] = strings.Trim(fields[c], " \t")
			}
			buf = append(buf, row)
			if len(buf) >= 5 {
				sql := InsertSql(table, tabcols, buf)
				//println(sql)
				_, err = db.Exec(sql)
				if err != nil {
					fmt.Println(err)
				}
				buf = make([]map[string]string, 0)
			}
		}

		if len(buf) > 0 {
			sql := InsertSql(table, tabcols, buf)
			//println(sql)
			_, err = db.Exec(sql)
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	log.Println("End\n")
}

func readCsvFile(file File) ([][]string, error) {
	filename := file.CsvFile
	delimiter := file.Delimiter
	hasHeader := file.HasHeader

	f, err := os.Open(filename)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	if delimiter != "" {
		r.Comma = []rune(delimiter)[0]
	}

	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	if hasHeader {
		records = records[1:]
	}

	return records, nil
}

func InsertSql(table string, columns []string, data []map[string]string) string {

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
