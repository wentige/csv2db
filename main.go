package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	//"github.com/kr/pretty"
	"github.com/joho/sqltocsv"
	"github.com/robfig/cron"
	"log"
	"os"
	"strconv"
	"strings"
	//"time"
)

var db *sql.DB

func main() {
	cfg := loadConfig()

	//db = openDatabase(cfg)
	//defer db.Close()

	c := cron.New()
	for _, file := range cfg.Files {
		fileInfo := file // This is IMPORTANT!
		c.AddFunc(file.RunAt, func() { importFile(fileInfo) })
	}
	c.Start()

	text := "Press Any Key to Exit..."
	fmt.Println(text)
	fmt.Scanln(&text)
}

func importFile(file FileConf) {
	filename := file.CsvFile
	table := file.Table

	log.Printf("Importing csv file '%s' into table '%s'\n", filename, table)
	return

	if file.Truncate {
		db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", table))
	}

	colMap := file.ColMap
	tabcols := make([]string, len(colMap))
	csvcols := make([]int, len(colMap))

	var i = 0
	for tabcolname, csvcolidx := range colMap {
		k, _ := strconv.Atoi(csvcolidx)
		tabcols[i] = tabcolname
		csvcols[i] = k - 1 // the index is 1-based
		i++
	}

	//pretty.Println(tabcols)
	//pretty.Println(csvcols)

	records, err := readCsvFile(file)
	if err != nil {
		log.Println(err)
	} else {
		buf := make([]map[string]string, 0)

		for _, fields := range records {
			row := make(map[string]string)

			for i, colname := range tabcols {
				c := csvcols[i]
				row[colname] = strings.Trim(fields[c], " \t")
			}

			buf = append(buf, row)
			if len(buf) >= 1000 {
				sql := insertSql(table, tabcols, buf)
				//println(sql)

				_, err = db.Exec(sql)
				if err != nil {
					log.Println(err)
				}

				buf = make([]map[string]string, 0)
			}
		}

		if len(buf) > 0 {
			sql := insertSql(table, tabcols, buf)
			//println(sql)

			_, err = db.Exec(sql)
			if err != nil {
				log.Println(err)
			}
		}
	}

	log.Println("End\n")
}

func readCsvFile(file FileConf) ([][]string, error) {
	f, err := os.Open(file.CsvFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	if file.Delimiter != "" {
		r.Comma = []rune(file.Delimiter)[0]
	}

	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	if file.HasHeader {
		records = records[1:]
	}

	//pretty.Println(records)

	return records, nil
}

func exportTable(table string) {
	sql := fmt.Sprintf("SELECT * FROM `%s`", table)
	rows, _ := db.Query(sql)
	//pretty.Println(rows)

	converter := sqltocsv.New(rows)
	converter.TimeFormat = "2006-01-02 15:04:05"
	filename := fmt.Sprintf("%s.csv", table)
	err := converter.WriteFile(filename)
	check(err)
}
