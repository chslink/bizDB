package gen

import (
	_ "embed"
	"fmt"
	"strings"
	"text/template"
)

type ModelInfo struct {
	StructName      string
	TableName       string
	ModelImportPath string
	PrimaryKey      struct {
		Name   string
		GoType string
	}
	UniqueIndexes []struct {
		FieldName string
		FieldType string
	}
	Fields []struct {
		Name   string
		GoType string
		Tag    string
	}
}

func getModelInfo(conf *Config, table string, cols []Column, indexes []IndexInfo) ModelInfo {
	info := ModelInfo{
		TableName:       table,
		StructName:      toCamelCase(table),
		ModelImportPath: conf.ModelImportPath, // 假设config中包含模型包路径配置
	}

	// 处理主键
	for _, col := range cols {
		if isPrimary(col, indexes) {
			info.PrimaryKey = struct {
				Name   string
				GoType string
			}{
				Name:   toCamelCase(col.Name),
				GoType: mysqlToGoType(col),
			}
			break // 假设单列主键
		}
	}

	// 处理唯一索引（仅处理单列唯一索引）
	uniqueIndexMap := make(map[string]bool)
	for _, idx := range indexes {
		// 过滤条件：唯一索引、非主键、单列
		if idx.IsUnique && !idx.IsPrimary && len(idx.ColumnNames) == 1 {
			colName := idx.ColumnNames[0]
			// 去重处理
			if !uniqueIndexMap[colName] {
				// 获取字段类型
				var fieldType string
				for _, c := range cols {
					if c.Name == colName {
						fieldType = mysqlToGoType(c)
						break
					}
				}

				info.UniqueIndexes = append(info.UniqueIndexes, struct {
					FieldName string
					FieldType string
				}{
					FieldName: toCamelCase(colName),
					FieldType: fieldType,
				})
				uniqueIndexMap[colName] = true
			}
		}
	}

	// 处理所有字段
	for _, col := range cols {
		field := struct {
			Name   string
			GoType string
			Tag    string
		}{
			Name:   toCamelCase(col.Name),
			GoType: mysqlToGoType(col),
			Tag:    fmt.Sprintf("`db:\"%s\"`", col.Name), // 生成DB标签
		}

		info.Fields = append(info.Fields, field)
	}

	return info
}

func isPrimary(col Column, indexes []IndexInfo) bool {

	for _, index := range indexes {

		if len(index.ColumnNames) > 0 && index.ColumnNames[0] == col.Name && index.IsPrimary {
			return true
		}
	}
	return false
}

//go:embed repo.tpl
var repoTpl string
var repoTemplate = template.Must(template.New("repo").Funcs(funcMap).Parse(repoTpl))

func generateRepoCode(model ModelInfo) (string, error) {
	var buf strings.Builder
	if err := repoTemplate.Execute(&buf, model); err != nil {
		return "", err
	}
	return buf.String(), nil
}
