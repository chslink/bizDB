package bizdb

const memoryRepoTemplate = `
package repos

import (
	"testCode/test/models"
	"testCode/test"

	"errors"
	"fmt"
)

var (
	Err{{.Name}}NotFound = errors.New("{{.Table | lower}} not found")
)

type {{.Name}}Repo struct {
	memDB     *test.MemoryDB
	tableName string
	tx        *test.Transaction // 事务对象
}

// New{{.Name}}Repo 创建普通Repository(无事务)
func New{{.Name}}Repo(memDB *test.MemoryDB) *{{.Name}}Repo {
	return &{{.Name}}Repo{
		memDB:     memDB,
		tableName: "{{.Table}}",
	}
}

// WithTx 创建带事务的Repository
func (r *{{.Name}}Repo) WithTx(tx *test.Transaction) *{{.Name}}Repo {
	return &{{.Name}}Repo{
		memDB:     r.memDB,
		tableName: r.tableName,
		tx:        tx,
	}
}

type {{.Name}}Query struct {
	{{range .Fields}}
	{{.Name}} *{{.Type}}{{end}}
	Limit  *int
	Offset *int
}

// GetByID 根据主键获取
func (r *{{.Name}}Repo) GetByID(id {{primaryKeyType .Fields}}) (*models.{{.Name}}, error) {
	val, err := r.memDB.Get(r.tx, r.tableName, id)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, Err{{.Name}}NotFound
	}

	model, ok := val.(*models.{{.Name}})
	if !ok {
		return nil, fmt.Errorf("type assertion failed")
	}

	return model.Copy(), nil
}

{{range .Fields}}
{{if .Unique}}
// GetBy{{.Name}} 根据唯一字段查询
func (r *{{$.Name}}Repo) GetBy{{.Name}}(val {{.Type}}) (*models.{{$.Name}}, error) {
	query := {{$.Name}}Query{
		{{.Name}}: &val,
	}
	
	results, err := r.Query(query)
	if err != nil {
		return nil, err
	}
	
	if len(results) == 0 {
		return nil, Err{{$.Name}}NotFound
	}
	
	return results[0], nil
}
{{end}}
{{end}}

// Create 创建记录
func (r *{{.Name}}Repo) Create(model *models.{{.Name}}) error {
	if r.tx == nil {
		return errors.New("write operation requires a transaction")
	}

	// 检查主键是否已存在
	if existing, _ := r.memDB.Get(r.tx, r.tableName, model.{{getPrimaryKey .Fields}}); existing != nil {
		return fmt.Errorf("record with id %v already exists", model.{{getPrimaryKey .Fields}})
	}

	// 存储拷贝
	return r.memDB.Put(r.tx, r.tableName, model.{{getPrimaryKey .Fields}}, model.Copy())
}

// Update 更新记录
func (r *{{.Name}}Repo) Update(model *models.{{.Name}}) error {
	if r.tx == nil {
		return errors.New("write operation requires a transaction")
	}

	// 检查记录是否存在
	if existing, _ := r.memDB.Get(r.tx, r.tableName, model.{{getPrimaryKey .Fields}}); existing == nil {
		return Err{{.Name}}NotFound
	}

	// 更新为拷贝
	return r.memDB.Put(r.tx, r.tableName, model.{{getPrimaryKey .Fields}}, model.Copy())
}

// Delete 删除记录
func (r *{{.Name}}Repo) Delete(id {{primaryKeyType .Fields}}) error {
	if r.tx == nil {
		return errors.New("delete operation requires a transaction")
	}

	// 检查记录是否存在
	if existing, _ := r.memDB.Get(r.tx, r.tableName, id); existing == nil {
		return Err{{.Name}}NotFound
	}

	return r.memDB.Delete(r.tx, r.tableName, id)
}

// Query 高级查询
func (r *{{.Name}}Repo) Query(q {{.Name}}Query) ([]*models.{{.Name}}, error) {
	// 获取全表数据
	all, err := r.memDB.Scan(r.tx, r.tableName)
	if err != nil {
		return nil, err
	}

	var results []*models.{{.Name}}
	for _, val := range all {
		model, ok := val.(*models.{{.Name}})
		if !ok {
			continue
		}

		match := true
		{{range .Fields}}
		if q.{{.Name}} != nil && *q.{{.Name}} != model.{{.Name}} {
			match = false
		}{{end}}

		if match {
			results = append(results, model.Copy())
		}
	}

	// 分页处理
	if q.Limit != nil || q.Offset != nil {
		offset := 0
		if q.Offset != nil {
			offset = *q.Offset
		}

		limit := len(results)
		if q.Limit != nil {
			limit = *q.Limit
		}

		end := offset + limit
		if end > len(results) {
			end = len(results)
		}

		if offset > len(results) {
			results = []*models.{{.Name}}{}
		} else {
			results = results[offset:end]
		}
	}

	return results, nil
}

`
