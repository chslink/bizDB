package bizdb

import (
	"database/sql"
	"fmt"
	"strings"
)

// IndexInfo 新增索引信息结构体
type IndexInfo struct {
	IndexName    string
	ColumnNames  []string
	IsUnique     bool
	IsPrimary    bool
	IndexType    string
	IndexComment string
}

type Column struct {
	Name       string
	Type       string
	Nullable   string
	Comment    string
	ColumnType string       // 包含更多类型信息（如有符号/无符号）
	Indexes    []*IndexInfo // 新增索引关联
}

type ModelTemplateData struct {
	PackageName string
	StructName  string
	Imports     map[string]bool
	Columns     []FieldData
	Indexes     []IndexInfo // 新增顶层索引信息
}

type FieldData struct {
	Name      string
	Type      string
	Tag       string
	Comment   string
	IndexTags string // 新增索引标签
}

// 新增获取所有表的方法
func getAllTables(db *sql.DB, dbName string) ([]string, error) {
	query := `
		SELECT TABLE_NAME 
		FROM information_schema.TABLES 
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
	`

	rows, err := db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func getColumns(db *sql.DB, dbName, tableName string) ([]Column, error) {
	query := `
		SELECT 
			COLUMN_NAME,
			DATA_TYPE,
			COLUMN_TYPE,
			IS_NULLABLE,
			COLUMN_COMMENT
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		err := rows.Scan(
			&col.Name,
			&col.Type,
			&col.ColumnType,
			&col.Nullable,
			&col.Comment,
		)
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	return columns, nil
}

// 新增获取索引信息的方法
func getIndexes(db *sql.DB, dbName, tableName string) ([]IndexInfo, error) {
	query := `
		SELECT 
			INDEX_NAME,
			COLUMN_NAME,
			SEQ_IN_INDEX,
			IF(NON_UNIQUE = 0, 1, 0) AS IS_UNIQUE,
			IF(INDEX_NAME = 'PRIMARY', 1, 0) AS IS_PRIMARY,
			INDEX_TYPE,
			INDEX_COMMENT
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
	`

	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexMap := make(map[string]*IndexInfo)
	for rows.Next() {
		var (
			name      string
			column    string
			seq       int
			isUnique  bool
			isPrimary bool
			indexType string
			comment   string
		)

		err := rows.Scan(
			&name,
			&column,
			&seq,
			&isUnique,
			&isPrimary,
			&indexType,
			&comment,
		)
		if err != nil {
			return nil, err
		}

		if index, exists := indexMap[name]; exists {
			index.ColumnNames = append(index.ColumnNames, column)
		} else {
			indexMap[name] = &IndexInfo{
				IndexName:    name,
				ColumnNames:  []string{column},
				IsUnique:     isUnique,
				IsPrimary:    isPrimary,
				IndexType:    indexType,
				IndexComment: comment,
			}
		}
	}

	var indexes []IndexInfo
	for _, idx := range indexMap {
		// 过滤主键（通常已经在字段定义中处理）
		if !idx.IsPrimary {
			indexes = append(indexes, *idx)
		}
	}
	return indexes, nil
}

func formatIndexTag(idx IndexInfo) string {
	tag := ""
	switch {
	case idx.IsPrimary:
		tag = "pk"
	case idx.IsUnique:
		tag = fmt.Sprintf("unique:%s", idx.IndexName)
	default:
		tag = fmt.Sprintf("index:%s", idx.IndexName)
	}
	return tag
}

func getIndexTypeDescription(idx IndexInfo) string {
	switch {
	case idx.IsPrimary:
		return "PRIMARY KEY"
	case idx.IsUnique:
		return "UNIQUE INDEX"
	default:
		return fmt.Sprintf("%s INDEX", idx.IndexType)
	}
}

func toCamelCase(s string) string {
	parts := strings.Split(s, "_")
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		parts[i] = strings.Title(parts[i])
	}
	return strings.Join(parts, "")
}

func mysqlToGoType(col Column) string {
	nullable := col.Nullable == "YES"
	isUnsigned := strings.Contains(col.ColumnType, "unsigned")

	switch strings.ToLower(col.Type) {
	case "tinyint":
		if isUnsigned {
			return wrapType("uint8", nullable)
		}
		return wrapType("int8", nullable)
	case "smallint":
		if isUnsigned {
			return wrapType("uint16", nullable)
		}
		return wrapType("int16", nullable)
	case "mediumint", "int":
		if isUnsigned {
			return wrapType("uint32", nullable)
		}
		return wrapType("int32", nullable)
	case "bigint":
		if isUnsigned {
			return wrapType("uint64", nullable)
		}
		return wrapType("int64", nullable)
	case "float":
		return wrapType("float32", nullable)
	case "double", "decimal":
		return wrapType("float64", nullable)
	case "char", "varchar", "text", "enum", "set":
		return wrapType("string", nullable)
	case "json":
		return wrapType("string", nullable)
	case "binary", "varbinary", "blob":
		return wrapType("[]byte", nullable)
	case "date", "datetime", "timestamp", "time":
		return wrapType("time.Time", nullable)
	case "bit":
		return wrapType("[]byte", nullable)
	default:
		return wrapType("interface{}", nullable)
	}
}

func wrapType(t string, nullable bool) string {
	if nullable {
		switch t {
		case "string":
			return "sql.NullString"
		case "int32":
			return "sql.NullInt32"
		case "int64":
			return "sql.NullInt64"
		case "float64":
			return "sql.NullFloat64"
		case "bool":
			return "sql.NullBool"
		case "time.Time":
			return "sql.NullTime"
		default:
			return "*" + t
		}
	}
	return t
}
