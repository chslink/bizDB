package gen

import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
)

func Run(conf *Config) error {

	dbConf, err := mysql.ParseDSN(conf.Dsn)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", conf.Dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	// 获取要生成的数据表
	allTables, err := getAllTables(db, dbConf.DBName)
	if err != nil {
		return err
	}
	tables := Match(allTables, conf.Tables)
	if conf.ExcludeTables != "" {
		tables = Match(tables, conf.ExcludeTables, true)
	}
	// 创建生成路径
	_ = os.MkdirAll(conf.ModelPackage, 0755)
	if !conf.OnlyModel {
		_ = os.MkdirAll(conf.RepoPackage, 0755)
	}
	// 获取表格信息
	for _, table := range tables {
		columns, err := getColumns(db, dbConf.DBName, table)
		if err != nil {
			log.Printf("跳过表 %s: %v", table, err)
			continue
		}
		// 新增获取索引信息
		indexes, err := getIndexes(db, dbConf.DBName, table)
		if err != nil {
			log.Printf("获取表 %s 索引失败: %v", table, err)
			continue
		}
		code, err := generateModelCode(conf.ModelPackage, table, columns, indexes)
		if err != nil {
			log.Printf("生成model代码失败: %v", err)
			continue
		}
		filename := filepath.Join(conf.ModelDir, strings.ToLower(table)+".go")
		if err := os.WriteFile(filename, []byte(code), 0644); err != nil {
			log.Printf("写入model代码失败: %v", err)
			continue
		}
		if conf.OnlyModel {
			// 跳过repo生成
			continue
		}
		modelInfo := getModelInfo(conf, table, columns, indexes)
		code, err = generateRepoCode(modelInfo)
		if err != nil {
			log.Printf("生成repo代码失败: %v", err)
			continue
		}
		repoFilename := filepath.Join(conf.RepoDir, strings.ToLower(table)+"_repo.go")
		if err := os.WriteFile(repoFilename, []byte(code), 0644); err != nil {
			log.Printf("写入repo代码失败: %v", err)
			continue
		}
	}

	return nil
}

func Match(allTables []string, pattern string, exclude ...bool) []string {
	isExclude := false
	if len(exclude) > 0 {
		isExclude = exclude[0]
	}
	var tables []string
	for _, table := range allTables {
		ok := WildcardRegexMatch(table, pattern)
		if !isExclude && ok {
			tables = append(tables, table)
		}
		if isExclude && !ok {
			tables = append(tables, table)
		}
	}
	return tables
}

// WildcardRegexMatch 通配符转正则匹配
func WildcardRegexMatch(s, pattern string) bool {
	regexPattern := strings.ReplaceAll(pattern, ".", `\.`)
	regexPattern = strings.ReplaceAll(regexPattern, "*", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "?", ".")
	re := regexp.MustCompile("^" + regexPattern + "$")
	return re.MatchString(s)
}
