# MemoryDB

![Go Version](https://img.shields.io/badge/go-1.21%2B-blue)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/chslink/bizdb/ci.yml)
![Go Report Card](https://goreportcard.com/badge/github.com/chslink/bizdb)

MemoryDB 是一个基于 Go 语言开发的高性能内存数据库，支持 ACID 事务、并发控制和数据持久化。适用于需要快速数据访问和高并发的应用场景。

## 核心特性

- **ACID 事务支持** - 提供完整的事务原子性、一致性、隔离性和持久性保障
- **并发安全设计** - 基于 sync.Map 实现高效并发访问控制
- **内存优化存储** - 采用紧凑数据结构，支持自动数据分片
- **数据持久化** - 支持 MySQL 数据同步（参见 [mysql_sync.go](mysql_sync.go)）
- **代码生成工具** - 内置 DDL 到数据模型的自动生成（参考 [cmd/bizdb](cmd/bizdb)）

## 快速开始

### 安装要求

- Go 1.21 或更高版本

```sh
go get github.com/chslink/bizdb@latest
```

### 基本使用
（示例见 [memory_db_1_test.go](memory_db_1_test.go)）：
```go
package main

import (
	"fmt"
	"github.com/chslink/bizdb"
)

type testModel struct {
	ID   int
	Name string
}


func main() {
	db := bizdb.New()
	table := "test_table"
	// 基本 CRUD 操作
	err := db.Put(nil, table, "alice", &testModel{ID: 1, Name: "Alice"})
	if err != nil {
		panic(err)
	}

	data, err := db.Get(nil, table, "alice")
	if err == nil {
		fmt.Printf("User data: %s\n", data)
	}
}
```

### 事务示例

```go
// 开启事务
tx, err := db.BeginTx()
if err != nil {
	panic("事务启动失败")
}

defer tx.Rollback()

// 事务内操作
if err := tx.Put("order:202308", []byte(`{"amount":199.99}`)); err != nil {
	panic(err)
}

// 提交事务
if err := tx.Commit(); err != nil {
	panic("事务提交失败")
}
```

## 高级功能

### 数据模型生成

使用内置代码生成工具从 SQL DDL 生成 Go 数据模型：

```sh
cd cmd/bizdb/
go run gen.go -schema=../../example/schemas.sql
```

### MySQL 同步

配置 MySQL 连接实现数据持久化（示例见 [mysql_loader.go](mysql_loader.go)）：

```go
db.EnableMySQLSync(&bizdb.MySQLConfig{
	Host:     "localhost",
	Port:     3306,
	User:     "root",
	Password: "",
	Database: "bizdb",
})
```

## 开发指南

### 运行测试

```sh
go test -v ./... -cover
```

### 贡献代码

请阅读 [贡献指南](CONTRIBUTING.md) 了解代码规范和质量标准。

## 许可证

本项目采用 [MIT 许可证](LICENSE)