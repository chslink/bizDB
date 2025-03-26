package repos

import (
	"errors"
	"fmt"
	"time"

	"github.com/chslink/bizdb"
	"github.com/chslink/bizdb/test/models"
)

var (
	ErrUserNotFound = errors.New("users not found")
)

type UserRepo struct {
	memDB     *bizdb.MemoryDB
	tableName string
	tx        *bizdb.Transaction // 事务对象
}

// NewUserRepo 创建普通Repository(无事务)
func NewUserRepo(memDB *bizdb.MemoryDB) *UserRepo {
	return &UserRepo{
		memDB:     memDB,
		tableName: "users",
	}
}

// WithTx 创建带事务的Repository
func (r *UserRepo) WithTx(tx *bizdb.Transaction) *UserRepo {
	return &UserRepo{
		memDB:     r.memDB,
		tableName: r.tableName,
		tx:        tx,
	}
}

type UserQuery struct {
	ID        *int64
	Name      *string
	Email     *string
	Age       *int
	CreatedAt *time.Time
	Limit     *int
	Offset    *int
}

// GetByID 根据主键获取
func (r *UserRepo) GetByID(id int64) (*models.User, error) {
	val, err := r.memDB.Get(r.tx, r.tableName, id)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, ErrUserNotFound
	}

	model, ok := val.(*models.User)
	if !ok {
		return nil, fmt.Errorf("type assertion failed")
	}

	return model.Copy(), nil
}

// GetByEmail 根据唯一字段查询
func (r *UserRepo) GetByEmail(val string) (*models.User, error) {
	query := UserQuery{
		Email: &val,
	}

	results, err := r.Query(query)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, ErrUserNotFound
	}

	return results[0], nil
}

// Create 创建记录
func (r *UserRepo) Create(model *models.User) error {
	if r.tx == nil {
		return errors.New("write operation requires a transaction")
	}

	// 检查主键是否已存在
	if existing, _ := r.memDB.Get(r.tx, r.tableName, model.ID); existing != nil {
		return fmt.Errorf("record with id %v already exists", model.ID)
	}

	// 存储拷贝
	return r.memDB.Put(r.tx, r.tableName, model.ID, model.Copy())
}

// Update 更新记录
func (r *UserRepo) Update(model *models.User) error {
	if r.tx == nil {
		return errors.New("write operation requires a transaction")
	}

	// 检查记录是否存在
	if existing, _ := r.memDB.Get(r.tx, r.tableName, model.ID); existing == nil {
		return ErrUserNotFound
	}

	// 更新为拷贝
	return r.memDB.Put(r.tx, r.tableName, model.ID, model.Copy())
}

// Delete 删除记录
func (r *UserRepo) Delete(id int64) error {
	if r.tx == nil {
		return errors.New("delete operation requires a transaction")
	}

	// 检查记录是否存在
	if existing, _ := r.memDB.Get(r.tx, r.tableName, id); existing == nil {
		return ErrUserNotFound
	}

	return r.memDB.Delete(r.tx, r.tableName, id)
}

// Query 高级查询
func (r *UserRepo) Query(q UserQuery) ([]*models.User, error) {
	// 获取全表数据
	all, err := r.memDB.Scan(r.tx, r.tableName)
	if err != nil {
		return nil, err
	}

	var results []*models.User
	for _, val := range all {
		model, ok := val.(*models.User)
		if !ok {
			continue
		}

		match := true

		if q.ID != nil && *q.ID != model.ID {
			match = false
		}
		if q.Name != nil && *q.Name != model.Name {
			match = false
		}
		if q.Email != nil && *q.Email != model.Email {
			match = false
		}
		if q.Age != nil && *q.Age != model.Age {
			match = false
		}
		if q.CreatedAt != nil && *q.CreatedAt != model.CreatedAt {
			match = false
		}

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
			results = []*models.User{}
		} else {
			results = results[offset:end]
		}
	}

	return results, nil
}
