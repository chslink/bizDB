package bizdb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testModel struct {
	ID   int
	Name string
}

func TestMemoryDB_BasicOperations(t *testing.T) {
	db := NewMemoryDB()
	table := "test_table"

	// 测试非事务写入应该失败
	err := db.Put(nil, table, 1, &testModel{ID: 1, Name: "Alice"})
	assert.Error(t, err, "非事务写入应该失败")

	// 开始事务
	tx := db.Begin()

	// 测试写入
	model1 := &testModel{ID: 1, Name: "Alice"}
	err = db.Put(tx, table, 1, model1)
	assert.NoError(t, err, "写入应该成功")

	// 测试读取
	val, err := db.Get(tx, table, 1)
	assert.NoError(t, err, "读取应该成功")
	assert.Equal(t, model1, val, "读取的数据应该与写入的一致")

	// 测试更新
	model1Updated := &testModel{ID: 1, Name: "Alice Updated"}
	err = db.Put(tx, table, 1, model1Updated)
	assert.NoError(t, err, "更新应该成功")

	val, err = db.Get(tx, table, 1)
	assert.NoError(t, err)
	assert.Equal(t, model1Updated, val, "数据应该已更新")

	// 测试删除
	err = db.Delete(tx, table, 1)
	assert.NoError(t, err, "删除应该成功")

	val, err = db.Get(tx, table, 1)
	assert.Equal(t, ErrKeyNotFound, err, "删除后读取应该返回错误")
	assert.Nil(t, val, "删除后读取应该返回nil")

	// 测试提交
	err = tx.Commit()
	assert.NoError(t, err, "提交应该成功")

	// 提交后检查数据状态
	val, err = db.Get(nil, table, 1)
	assert.Equal(t, ErrKeyNotFound, err, "已删除的数据不应该存在")
}

func TestMemoryDB_TransactionRollback(t *testing.T) {
	db := NewMemoryDB()
	table := "test_table"

	// 初始数据
	tx0 := db.Begin()
	err := db.Put(tx0, table, 1, &testModel{ID: 1, Name: "Initial"})
	assert.NoError(t, err)
	err = tx0.Commit()
	assert.NoError(t, err)

	// 开始新事务
	tx := db.Begin()

	// 修改数据
	err = db.Put(tx, table, 1, &testModel{ID: 1, Name: "Updated"})
	assert.NoError(t, err)

	// 验证事务内能看到修改
	val, err := db.Get(tx, table, 1)
	assert.NoError(t, err)
	assert.Equal(t, "Updated", val.(*testModel).Name)

	// 回滚事务
	err = tx.Rollback()
	assert.NoError(t, err)

	// 验证回滚后数据恢复
	val, err = db.Get(nil, table, 1)
	assert.NoError(t, err)
	assert.Equal(t, "Initial", val.(*testModel).Name, "回滚后数据应该恢复")
}

func TestMemoryDB_Scan(t *testing.T) {
	db := NewMemoryDB()
	table := "test_table"

	// 准备测试数据
	models := []*testModel{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	// 写入初始数据
	txInit := db.Begin()
	for _, m := range models {
		err := db.Put(txInit, table, m.ID, m)
		assert.NoError(t, err)
	}
	err := txInit.Commit()
	assert.NoError(t, err)

	// 测试非事务Scan
	result, err := db.Scan(nil, table)
	assert.NoError(t, err)
	assert.Len(t, result, 3, "应该扫描到3条记录")

	// 测试事务内Scan
	tx := db.Begin()
	result, err = db.Scan(tx, table)
	assert.NoError(t, err)
	assert.Len(t, result, 3, "事务内应该看到3条记录")

	// 在事务中修改数据
	err = db.Put(tx, table, 4, &testModel{ID: 4, Name: "David"})
	assert.NoError(t, err)
	err = db.Delete(tx, table, 2)
	assert.NoError(t, err)

	// 验证事务内Scan能看到未提交的修改
	result, err = db.Scan(tx, table)
	assert.NoError(t, err)
	assert.Len(t, result, 3, "应该看到3条记录(新增1条，删除1条)")
	_, exists := result[4]
	assert.True(t, exists, "应该包含新增的记录")
	_, exists = result[2]
	assert.False(t, exists, "不应该包含已删除的记录")

	// 验证非事务Scan看不到未提交的修改
	result, err = db.Scan(nil, table)
	assert.NoError(t, err)
	_, exists = result[4]
	assert.False(t, exists, "非事务Scan不应该看到未提交的新增记录")
	_, exists = result[2]
	assert.True(t, exists, "非事务Scan应该看到未删除的记录")

	// 提交后验证
	err = tx.Commit()
	assert.NoError(t, err)
	result, err = db.Scan(nil, table)
	assert.NoError(t, err)
	assert.Len(t, result, 3, "提交后应该有3条记录")
}

func TestMemoryDB_Concurrency(t *testing.T) {
	db := NewMemoryDB()
	table := "concurrent_table"
	const numWorkers = 10
	const numIterations = 100

	// 初始数据
	txInit := db.Begin()
	err := db.Put(txInit, table, "counter", 0)
	assert.NoError(t, err)
	err = txInit.Commit()
	assert.NoError(t, err)

	// 并发递增计数器
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				for {
					tx := db.Begin()
					val, err := db.Get(tx, table, "counter")
					if err != nil {
						tx.Rollback()
						continue
					}
					counter := val.(int)
					err = db.Put(tx, table, "counter", counter+1)
					if err != nil {
						tx.Rollback()
						continue
					}
					err = tx.Commit()
					if err == nil {
						break
					}
					// 如果是写冲突，重试
					if err != ErrWriteConflict {
						tx.Rollback()
						break
					}
				}
			}
		}()
	}

	wg.Wait()

	// 验证最终结果
	val, err := db.Get(nil, table, "counter")
	assert.NoError(t, err)
	assert.Equal(t, numWorkers*numIterations, val.(int), "计数器值不正确")
}

func TestMemoryDB_ConcurrentConflicts(t *testing.T) {
	db := NewMemoryDB()
	table := "conflict_table"
	const numWorkers = 5

	// 初始数据
	txInit := db.Begin()
	err := db.Put(txInit, table, "data", "initial")
	assert.NoError(t, err)
	err = txInit.Commit()
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	successCount := 0
	var successLock sync.Mutex

	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			tx := db.Begin()
			val, err := db.Get(tx, table, "data")
			assert.NoError(t, err)

			// 模拟处理延迟
			time.Sleep(time.Millisecond * 10)

			err = db.Put(tx, table, "data", val.(string)+fmt.Sprintf("-%d", id))
			assert.NoError(t, err)

			err = tx.Commit()
			if err == nil {
				successLock.Lock()
				successCount++
				successLock.Unlock()
			} else {
				assert.Equal(t, ErrWriteConflict, err)
			}
		}(i)
	}

	wg.Wait()

	assert.Equal(t, 1, successCount, "只有一个事务应该成功提交")

	val, err := db.Get(nil, table, "data")
	assert.NoError(t, err)
	assert.Contains(t, val.(string), "initial-", "数据应该被其中一个worker修改")
}

func TestMemoryDB_EdgeCases(t *testing.T) {
	db := NewMemoryDB()

	// 测试不存在的表
	val, err := db.Get(nil, "non_existent_table", 1)
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// 测试重复提交
	tx := db.Begin()
	err = tx.Commit()
	assert.NoError(t, err)
	err = tx.Commit()
	assert.Error(t, err, "重复提交应该失败")

	// 测试重复回滚
	tx = db.Begin()
	err = tx.Rollback()
	assert.NoError(t, err)
	err = tx.Rollback()
	assert.Error(t, err, "重复回滚应该失败")

	// 测试无效事务操作
	err = db.Put(nil, "table", 1, "value")
	assert.Error(t, err, "无事务写入应该失败")

	invalidTx := &Transaction{id: 999, memDB: db, updates: map[string]map[interface{}]interface{}{}}
	err = db.Put(invalidTx, "table", 1, "value")
	assert.Equal(t, ErrTransactionNotFound, err, "无效事务应该返回错误")

	// 测试空值存储
	tx = db.Begin()
	err = db.Put(tx, "table", "empty", nil)
	assert.NoError(t, err)
	val, err = db.Get(tx, "table", "empty")
	assert.NoError(t, err)
	assert.Nil(t, val)
	err = tx.Commit()
	assert.NoError(t, err)
}

func BenchmarkMemoryDB_Put(b *testing.B) {
	db := NewMemoryDB()
	table := "bench_table"
	model := &testModel{ID: 1, Name: "test"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.Begin()
		_ = db.Put(tx, table, i, model)
		_ = tx.Commit()
	}
}

func BenchmarkMemoryDB_Get(b *testing.B) {
	db := NewMemoryDB()
	table := "bench_table"
	tx := db.Begin()
	_ = db.Put(tx, table, 1, &testModel{ID: 1, Name: "test"})
	_ = tx.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get(nil, table, 1)
	}
}

func BenchmarkMemoryDB_Concurrent(b *testing.B) {
	db := NewMemoryDB()
	table := "bench_table"
	const numWorkers = 10

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx := db.Begin()
			val, err := db.Get(tx, table, 1)
			if err == nil {
				_ = db.Put(tx, table, 1, val)
			}
			_ = tx.Commit()
		}
	})
}
