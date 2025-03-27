package bizdb

import (
	"errors"
	"sync"
	"testing"
)

// 测试模型
type TestModel struct {
	ID    string
	Name  string
	Value int
}

func (t *TestModel) TableName() string {
	return "test_table"
}

func (t *TestModel) Indexes() []Index {
	return []Index{
		{
			Name:   "name_idx",
			Fields: []string{"Name"},
			Unique: false,
			Type:   "btree",
		},
	}
}

func (t *TestModel) Copy() *TestModel {
	return &TestModel{
		ID:    t.ID,
		Name:  t.Name,
		Value: t.Value,
	}
}

func TestMemoryDB_BasicOperations2(t *testing.T) {
	db := NewMemoryDB()
	key := "test_key"
	value := &TestModel{ID: "1", Name: "test", Value: 100}

	// 测试Put和Get
	tx := db.Begin()
	if err := db.Put(tx, "test_table", key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := db.Get(tx, "test_table", key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.(*TestModel).ID != value.ID {
		t.Errorf("Get returned wrong value, got %v, want %v", got, value)
	}

	// 测试Delete
	if err := db.Delete(tx, "test_table", key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = db.Get(tx, "test_table", key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Expected ErrKeyNotFound after delete, got %v", err)
	}

	// 测试Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestMemoryDB_TransactionIsolation(t *testing.T) {
	db := NewMemoryDB()
	key := "tx_key"
	initialValue := &TestModel{ID: "1", Name: "initial", Value: 100}

	// 初始数据
	initTx := db.Begin()
	if err := db.Put(initTx, "test_table", key, initialValue); err != nil {
		t.Fatalf("Initial Put failed: %v", err)
	}
	if err := initTx.Commit(); err != nil {
		t.Fatalf("Initial Commit failed: %v", err)
	}

	// 事务1读取
	tx1 := db.Begin()
	_, err := db.Get(tx1, "test_table", key)
	if err != nil {
		t.Fatalf("Tx1 Get failed: %v", err)
	}

	// 事务2修改并提交
	tx2 := db.Begin()
	updatedValue := &TestModel{ID: "1", Name: "updated", Value: 200}
	if err := db.Put(tx2, "test_table", key, updatedValue); err != nil {
		t.Fatalf("Tx2 Put failed: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Tx2 Commit failed: %v", err)
	}

	// 事务1再次读取应该看到旧值
	val, err := db.Get(tx1, "test_table", key)
	if err != nil {
		t.Fatalf("Tx1 second Get failed: %v", err)
	}
	if val.(*TestModel).Name != "initial" {
		t.Errorf("Tx1 should see old value, got %v, want initial", val)
	}

	// 事务1提交应该失败(写冲突)
	if err := tx1.Commit(); !errors.Is(err, ErrWriteConflict) {
		t.Errorf("Expected ErrWriteConflict, got %v", err)
	}
}

func TestMemoryDB_ConcurrentTransactions(t *testing.T) {
	db := NewMemoryDB()
	key := "concurrent_key"
	initialValue := &TestModel{ID: "1", Name: "initial", Value: 0}

	// 初始数据
	initTx := db.Begin()
	if err := db.Put(initTx, "test_table", key, initialValue); err != nil {
		t.Fatalf("Initial Put failed: %v", err)
	}
	if err := initTx.Commit(); err != nil {
		t.Fatalf("Initial Commit failed: %v", err)
	}

	var wg sync.WaitGroup
	successCount := 0
	mu := sync.Mutex{}

	// 启动多个并发事务尝试递增Value
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := db.Begin()
			val, err := db.Get(tx, "test_table", key)
			if err != nil {
				t.Logf("Get failed: %v", err)
				return
			}

			model := val.(*TestModel).Copy()
			model.Value++

			if err := db.Put(tx, "test_table", key, model); err != nil {
				t.Logf("Put failed: %v", err)
				return
			}

			if err := tx.Commit(); err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// 验证最终结果
	finalTx := db.Begin()
	finalVal, err := db.Get(finalTx, "test_table", key)
	if err != nil {
		t.Fatalf("Final Get failed: %v", err)
	}

	if finalVal.(*TestModel).Value != successCount {
		t.Errorf("Expected final Value to be %d, got %d", successCount, finalVal.(*TestModel).Value)
	}
}

func TestMemoryDB_ErrorCases(t *testing.T) {
	db := NewMemoryDB()

	// 测试无效事务
	invalidTx := &Transaction{id: 999, memDB: db}
	_, err := db.Get(invalidTx, "test_table", "key")
	if !errors.Is(err, ErrTransactionNotFound) {
		t.Errorf("Expected ErrTransactionNotFound, got %v", err)
	}

	// 测试已提交事务
	tx := db.Begin()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	_, err = db.Get(tx, "test_table", "key")
	if !errors.Is(err, ErrTransactionNotFound) {
		t.Errorf("Expected ErrTransactionNotFound, got %v", err)
	}

	// 测试不存在的键
	nonExistentTx := db.Begin()
	_, err = db.Get(nonExistentTx, "test_table", "non_existent_key")
	if !errors.Is(err, ErrTableNotFound) {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// 测试不存在的表
	_, err = db.Get(nil, "non_existent_table", "key")
	if !errors.Is(err, ErrTableNotFound) {
		t.Errorf("Expected ErrKeyNotFound for non-existent table, got %v", err)
	}
}

func TestMemoryDB_Range(t *testing.T) {
	db := NewMemoryDB()
	items := map[string]*TestModel{
		"key1": {ID: "1", Name: "one", Value: 1},
		"key2": {ID: "2", Name: "two", Value: 2},
		"key3": {ID: "3", Name: "three", Value: 3},
	}

	// 插入测试数据
	tx := db.Begin()
	for k, v := range items {
		if err := db.Put(tx, "test_table", k, v); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 测试Range
	var foundKeys []string
	tx = db.Begin()
	err := db.Range(tx, "test_table", func(id, val any) bool {
		foundKeys = append(foundKeys, id.(string))
		return true // 继续迭代
	})
	if err != nil {
		t.Fatalf("Range failed: %v", err)
	}

	if len(foundKeys) != len(items) {
		t.Errorf("Range returned wrong number of items, got %d, want %d", len(foundKeys), len(items))
	}
}

func TestMemoryDB_TransactionRollback2(t *testing.T) {
	db := NewMemoryDB()
	key := "rollback_key"
	initialValue := &TestModel{ID: "1", Name: "initial", Value: 100}

	// 初始数据
	initTx := db.Begin()
	if err := db.Put(initTx, "test_table", key, initialValue); err != nil {
		t.Fatalf("Initial Put failed: %v", err)
	}
	if err := initTx.Commit(); err != nil {
		t.Fatalf("Initial Commit failed: %v", err)
	}

	// 开始事务并修改数据
	tx := db.Begin()
	updatedValue := &TestModel{ID: "1", Name: "updated", Value: 200}
	if err := db.Put(tx, "test_table", key, updatedValue); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 验证事务内看到更新
	val, err := db.Get(tx, "test_table", key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val.(*TestModel).Name != "updated" {
		t.Errorf("Expected updated value in transaction, got %v", val)
	}

	// 回滚事务
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// 验证数据未改变
	finalTx := db.Begin()
	val, err = db.Get(finalTx, "test_table", key)
	if err != nil {
		t.Fatalf("Final Get failed: %v", err)
	}
	if val.(*TestModel).Name != "initial" {
		t.Errorf("Expected initial value after rollback, got %v", val)
	}
}

func TestMemoryDB_TableVersion(t *testing.T) {
	db := NewMemoryDB()
	key := "version_key"
	value := &TestModel{ID: "1", Name: "test", Value: 100}

	// 获取初始表版本
	tbl, _ := db.tables.LoadOrStore("test_table", &Table{data: make(map[interface{}]*Record)})
	tableObj := tbl.(*Table)
	initialVersion := tableObj.version

	// 修改数据
	tx := db.Begin()
	if err := db.Put(tx, "test_table", key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 验证版本号增加
	if tableObj.version <= initialVersion {
		t.Errorf("Expected version to increase, got %d (was %d)", tableObj.version, initialVersion)
	}
}
