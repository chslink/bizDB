package gen

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

type Field struct {
	Name    string `yaml:"name"`
	Type    string `yaml:"type"`
	Primary bool   `yaml:"primary"`
	Size    int    `yaml:"size"`
	Unique  bool   `yaml:"unique"`
	Comment string `yaml:"comment"`
}

type Model struct {
	Name   string  `yaml:"name"`
	Table  string  `yaml:"table"`
	Fields []Field `yaml:"fields"`
}

const modelTemplate = `
package models

import (
	"time"
	{{if hasSpecialTypes .Fields}}
	"encoding/json"
	{{end}}
)

type {{.Name}} struct {
	{{range .Fields}}
	{{.Name}} {{.Type}}  {{end}}
}

func ({{.Name}}) TableName() string {
	return "{{.Table}}"
}

// Copy 创建结构体的深拷贝
func (m *{{.Name}}) Copy() *{{.Name}} {
	if m == nil {
		return nil
	}
	
	cpy := &{{.Name}}{
		{{range .Fields}}
		{{.Name}}: {{if isPrimitive .Type}}m.{{.Name}}{{else if eq .Type "time.Time"}}m.{{.Name}}{{else}}deepCopy{{.Name}}(m.{{.Name}}){{end}},{{end}}
	}
	
	return cpy
}

{{range .Fields}}
{{if not (isPrimitive .Type)}}
func deepCopy{{.Name}}(src {{.Type}}) {{.Type}} {
	// 处理特殊类型的深拷贝逻辑
	{{if isSlice .Type}}
	if src == nil {
		return nil
	}
	dst := make({{.Type}}, len(src))
	copy(dst, src)
	return dst
	{{else if isMap .Type}}
	if src == nil {
		return nil
	}
	dst := make({{.Type}})
	for k, v := range src {
		dst[k] = v // 如果值也需要深拷贝，这里需要递归处理
	}
	return dst
	{{else if isPointer .Type}}
	if src == nil {
		return nil
	}
	// 这里需要根据具体类型处理
	{{else}}
	// 默认使用JSON序列化/反序列化作为通用深拷贝方案
	var dst {{.Type}}
	bytes, _ := json.Marshal(src)
	_ = json.Unmarshal(bytes, &dst)
	return dst
	{{end}}
}
{{end}}
{{end}}
`

func isPrimitive(typ string) bool {
	switch typ {
	case "string", "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64",
		"float32", "float64", "bool":
		return true
	default:
		return false
	}
}

func isSlice(typ string) bool {
	return strings.HasPrefix(typ, "[]")
}

func isMap(typ string) bool {
	return strings.Contains(typ, "map[")
}

func isPointer(typ string) bool {
	return strings.HasPrefix(typ, "*")
}

func hasSpecialTypes(fields []Field) bool {
	for _, f := range fields {
		if !isPrimitive(f.Type) && f.Type != "time.Time" {
			return true
		}
	}
	return false
}

func getPrimaryKey(fields []Field) string {
	for _, f := range fields {
		if f.Primary {
			return f.Name
		}
	}
	return ""
}

func primaryKeyType(fields []Field) string {
	for _, f := range fields {
		if f.Primary {
			return f.Type
		}
	}
	return ""
}

var funcs = template.FuncMap{
	"hasSpecialTypes": hasSpecialTypes,
	"isSlice":         isSlice,
	"isMap":           isMap,
	"isPointer":       isPointer,
	"isPrimitive":     isPrimitive,
	"lower":           strings.ToLower,
	"getPrimaryKey":   getPrimaryKey,
	"primaryKeyType":  primaryKeyType,
}

func GenerateCode(model Model, outputPath string) error {
	// 创建输出目录
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return err
	}

	// 生成模型文件
	modelFile := filepath.Join(outputPath, strings.ToLower(model.Table)+".go")
	modelTmpl := template.Must(template.New("model").Funcs(funcs).Parse(modelTemplate))

	var modelBuf bytes.Buffer
	if err := modelTmpl.Execute(&modelBuf, model); err != nil {
		return err
	}

	// 格式化代码
	formatted, err := format.Source(modelBuf.Bytes())
	if err != nil {
		return fmt.Errorf("formatting model code: %w", err)
	}

	if err := os.WriteFile(modelFile, formatted, 0644); err != nil {
		return err
	}

	// 生成仓库文件
	repoTmpl := template.Must(template.New("repo").Funcs(funcs).Parse(memoryRepoTemplate))
	var repoBuf bytes.Buffer
	if err := repoTmpl.Execute(&repoBuf, model); err != nil {
		return err
	}
	repoPath := filepath.Join(outputPath, "../repos", model.Table+"_repo.go")
	formatted, err = format.Source(repoBuf.Bytes())
	if err != nil {
		return fmt.Errorf("formatting model code: %w", err)
	}

	if err := os.WriteFile(repoPath, formatted, 0644); err != nil {
		return err
	}

	return nil
}
