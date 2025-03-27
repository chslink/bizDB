package example

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/chslink/bizdb/gen"
)

func TestExample(t *testing.T) {
	dsn := "root:f_trunk@tcp(localhost:3306)/biz"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	// 写入数据库
	schemas, err := os.ReadFile("schemas.sql")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(string(schemas))
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	conf := &gen.Config{
		Dsn:             dsn,
		ModelDir:        "./models",
		ModelPackage:    "models",
		RepoDir:         "./repos",
		RepoPackage:     "repos",
		OnlyModel:       false,
		Tables:          "*",
		ExcludeTables:   "test_*",
		ModelImportPath: "github.com/chslink/bizdb/example/models",
	}
	err = gen.Run(conf)
	if err != nil {
		t.Fatal(err)
	}
}
