package main

import (
	//"fmt"
	"github.com/BurntSushi/toml"
	//"github.com/kr/pretty"
	"io/ioutil"
)

type DatabaseConf struct {
	Host     string `toml:"host"`
	Port     string `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	Name     string `toml:"dbname"`
}

type FileConf struct {
	CsvFile   string            `toml:"csvfile"`
	Delimiter string            `toml:"delimiter"`
	HasHeader bool              `toml:"hasheader"`
	Table     string            `toml:"table"`
	Truncate  bool              `toml:"truncate"`
	RunAt     string            `toml:"runat"`
	ColMap    map[string]string `toml:"colmap"`
}

// ./config.toml
type Config struct {
	Database DatabaseConf `toml:"Database"`
	Files    []FileConf   `toml:"Files"`
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
