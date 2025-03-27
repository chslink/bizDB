package gen

import (
	"database/sql"
	_ "embed"
	"fmt"
	"sort"
	"strings"
	"text/template"
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
	TableName   string
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

//go:embed model.tpl
var modelTpl string
var modelTemplate = template.Must(template.New("model").Funcs(funcMap).Parse(modelTpl))

// 添加自定义模板函数
var funcMap = template.FuncMap{
	"join":          strings.Join,
	"indexType":     getIndexTypeDescription,
	"toCamel":       toCamelCase,
	"upper":         strings.ToUpper,
	"toLower":       strings.ToLower,
	"isPointerType": isPointerType,
	"toRepoName":    func(s string) string { return strings.ToUpper(s[:1]) + s[1:] + "Repo" },
	"isLast":        func(index int, total int) bool { return index == total-1 },
	"add":           func(a, b int) int { return a + b },
	"isLastField":   func(idx, total int) bool { return idx == total-1 },
	"isLastIndex":   func(idx, total int) bool { return idx == total-1 },
	"toGoComments":  func(s string) string { return "// " + strings.ReplaceAll(s, "\n", "\n// ") },
}

func generateModelCode(pkgName, tableName string, columns []Column, indexes []IndexInfo) (string, error) {
	imports := make(map[string]bool)
	var fields []FieldData

	structName := toCamelCase(tableName)

	// 创建字段到索引的映射
	indexMap := make(map[string][]string)
	for _, idx := range indexes {
		for _, col := range idx.ColumnNames {
			indexMap[col] = append(indexMap[col], formatIndexTag(idx))
		}
	}

	for _, col := range columns {
		goType := mysqlToGoType(col)
		fieldName := toCamelCase(col.Name)
		// 处理索引标签
		var indexTags []string
		if tags, exists := indexMap[col.Name]; exists {
			indexTags = append(indexTags, tags...)
		}
		indexTagStr := strings.Join(indexTags, " ")
		// 处理需要导入的包
		switch goType {
		case "time.Time", "*time.Time":
			imports["time"] = true
		case "sql.NullString", "sql.NullInt32", "sql.NullInt64", "sql.NullFloat64", "sql.NullBool", "sql.NullTime":
			imports["database/sql"] = true
		}

		// 生成结构体标签
		tags := fmt.Sprintf("`db:\"%s\" json:\"%s\"`",
			col.Name,
			col.Name,
		)

		// 处理注释
		comment := ""
		if col.Comment != "" {
			comment = fmt.Sprintf("// %s", col.Comment)
		}

		fields = append(fields, FieldData{
			Name:      fieldName,
			Type:      goType,
			Tag:       tags,
			Comment:   comment,
			IndexTags: indexTagStr, // 添加索引标签
		})
	}
	// 更新模板数据
	tmplData := ModelTemplateData{
		PackageName: pkgName,
		StructName:  structName,
		TableName:   tableName,
		Imports:     imports,
		Columns:     fields,
		Indexes:     indexes,
	}

	var buf strings.Builder
	err := modelTemplate.Execute(&buf, tmplData)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
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
		indexes = append(indexes, *idx)

	}

	return getOrderedColumns(indexes), nil
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
			return wrapType("uint64", nullable)
		}
		return wrapType("int64", nullable)
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

// 类型判断函数
func isPointerType(goType string) bool {
	// 识别需要深拷贝的类型
	needDeepCopy := []string{
		"*",
		"sql.Null",
		"datatypes.",
		"json.RawMessage",
	}
	for _, prefix := range needDeepCopy {
		if strings.HasPrefix(goType, prefix) {
			return true
		}
	}
	return false
}

// 自动处理复合索引字段排序
func getOrderedColumns(indexes []IndexInfo) []IndexInfo {
	sort.Slice(indexes, func(i, j int) bool {
		if indexes[i].IsPrimary {
			return true
		}
		if indexes[j].IsPrimary {
			return false
		}
		return indexes[i].IndexName < indexes[j].IndexName
	})
	return indexes
}
