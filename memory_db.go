package bizdb

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
)

var (
	ErrTransactionNotFound = errors.New("transaction not found")
	ErrKeyNotFound         = errors.New("key not found")
	ErrWriteConflict       = errors.New("write conflict detected")
)

type Record struct {
	value   any
	version int64 // 每个键的版本号
}

type IModel interface {
	TableName() string
	Indexes() []string
}

type Copyable[T any] interface {
	Copy() T
}

type Index struct {
	entries map[interface{}]map[interface{}]struct{} // 索引值 -> 主键集合
}

type Table struct {
	mu      sync.RWMutex
	data    map[interface{}]*Record
	indexes map[string]*Index // 索引名到索引的映射
	version int64             // 表的版本号，用于快速检测变更
}

type Transaction struct {
	id           int64
	memDB        *MemoryDB
	keySnapshots map[string]map[interface{}]int64 // 记录访问键的版本号
	updates      map[string]map[interface{}]any
	deletes      map[string]map[interface{}]struct{}
	committed    bool
}

type MemoryDB struct {
	tables    sync.Map // 表名 -> *Table
	txCounter int64
	activeTx  sync.Map // 新增：跟踪活动事务
}

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{
		tables:    sync.Map{},
		txCounter: 0,
		activeTx:  sync.Map{},
	}
}

func (db *MemoryDB) Begin() *Transaction {
	txID := atomic.AddInt64(&db.txCounter, 1)
	tx := &Transaction{
		id:           txID,
		memDB:        db,
		keySnapshots: make(map[string]map[interface{}]int64),
		updates:      make(map[string]map[interface{}]any),
		deletes:      make(map[string]map[interface{}]struct{}),
	}
	db.activeTx.Store(txID, tx)
	return tx
}

func (db *MemoryDB) Get(tx *Transaction, table string, key interface{}) (interface{}, error) {
	if tx == nil {
		tbl, ok := db.tables.Load(table)
		if !ok {
			return nil, ErrKeyNotFound
		}
		tableObj := tbl.(*Table)
		tableObj.mu.RLock()
		defer tableObj.mu.RUnlock()
		record, exists := tableObj.data[key]
		if !exists {
			return nil, ErrKeyNotFound
		}
		return record.value, nil
	}
	// 新增有效性检查
	if _, ok := db.activeTx.Load(tx.id); !ok {
		return nil, ErrTransactionNotFound
	}

	if tx.committed {
		return nil, errors.New("transaction already committed")
	}

	if deletes, ok := tx.deletes[table]; ok && deletes != nil {
		if _, deleted := deletes[key]; deleted {
			return nil, ErrKeyNotFound
		}
	}

	if updates, ok := tx.updates[table]; ok && updates != nil {
		if val, updated := updates[key]; updated {
			return val, nil
		}
	}

	tbl, ok := db.tables.Load(table)
	if !ok {
		return nil, ErrKeyNotFound
	}
	tableObj := tbl.(*Table)
	tableObj.mu.RLock()
	defer tableObj.mu.RUnlock()

	record, exists := tableObj.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	if tx.keySnapshots[table] == nil {
		tx.keySnapshots[table] = make(map[interface{}]int64)
	}
	tx.keySnapshots[table][key] = record.version
	return record.value, nil
}

func (db *MemoryDB) Put(tx *Transaction, table string, key interface{}, value any) error {
	if tx == nil {
		return errors.New("write operation requires a transaction")
	}
	// 新增有效性检查
	if _, ok := db.activeTx.Load(tx.id); !ok {
		return ErrTransactionNotFound
	}

	if tx.committed {
		return errors.New("transaction already committed")
	}

	if tx.updates[table] == nil {
		tx.updates[table] = make(map[interface{}]any)
	}
	tx.updates[table][key] = value

	if tx.deletes[table] != nil {
		delete(tx.deletes[table], key)
	}

	return nil
}

func (db *MemoryDB) Delete(tx *Transaction, table string, key interface{}) error {
	if tx == nil {
		return errors.New("delete operation requires a transaction")
	}
	// 新增有效性检查
	if _, ok := db.activeTx.Load(tx.id); !ok {
		return ErrTransactionNotFound
	}

	if tx.committed {
		return errors.New("transaction already committed")
	}

	if tx.deletes[table] == nil {
		tx.deletes[table] = make(map[interface{}]struct{})
	}
	tx.deletes[table][key] = struct{}{}

	if tx.updates[table] != nil {
		delete(tx.updates[table], key)
	}

	return nil
}

func (db *MemoryDB) Scan(tx *Transaction, table string) (map[interface{}]interface{}, error) {
	if tx == nil {
		tbl, ok := db.tables.Load(table)
		if !ok {
			return nil, ErrKeyNotFound
		}
		tableObj := tbl.(*Table)
		tableObj.mu.RLock()
		defer tableObj.mu.RUnlock()

		result := make(map[interface{}]interface{})
		for k, v := range tableObj.data {
			result[k] = v.value
		}
		return result, nil
	}

	if tx.committed {
		return nil, errors.New("transaction already committed")
	}

	result := make(map[interface{}]interface{})

	tbl, ok := db.tables.Load(table)
	if ok {
		tableObj := tbl.(*Table)
		tableObj.mu.RLock()
		for k, v := range tableObj.data {
			if deletes, ok := tx.deletes[table]; ok && deletes != nil {
				if _, deleted := deletes[k]; deleted {
					continue
				}
			}
			result[k] = v.value
		}
		tableObj.mu.RUnlock()
	}

	if updates, ok := tx.updates[table]; ok {
		for k, v := range updates {
			result[k] = v
		}
	}

	return result, nil
}

func (tx *Transaction) Commit() error {
	if tx.committed {
		return errors.New("transaction already committed")
	}

	// 收集所有涉及的表
	tables := make(map[string]struct{})
	for table := range tx.keySnapshots {
		tables[table] = struct{}{}
	}
	for table := range tx.updates {
		tables[table] = struct{}{}
	}
	for table := range tx.deletes {
		tables[table] = struct{}{}
	}

	var tableNames []string
	for table := range tables {
		tableNames = append(tableNames, table)
	}
	sort.Strings(tableNames)

	var lockedTables []*Table
	for _, table := range tableNames {
		tbl, _ := tx.memDB.tables.LoadOrStore(table, &Table{data: make(map[interface{}]*Record)})
		tableObj := tbl.(*Table)
		tableObj.mu.Lock()
		lockedTables = append(lockedTables, tableObj)
	}
	defer func() {
		for _, t := range lockedTables {
			t.mu.Unlock()
		}
	}()

	for table, keys := range tx.keySnapshots {
		tbl, ok := tx.memDB.tables.Load(table)
		if !ok {
			return ErrWriteConflict
		}
		tableObj := tbl.(*Table)
		for key, snapVer := range keys {
			record, exists := tableObj.data[key]
			if !exists && snapVer != 0 {
				return ErrWriteConflict
			}
			if exists && record.version != snapVer {
				return ErrWriteConflict
			}
		}
	}

	for table, updates := range tx.updates {
		tbl, _ := tx.memDB.tables.LoadOrStore(table, &Table{data: make(map[interface{}]*Record)})
		tableObj := tbl.(*Table)
		for key, value := range updates {
			if record, exists := tableObj.data[key]; exists {
				record.value = value
				record.version++
				//removeIndexes(tableObj, record, key)
			} else {
				tableObj.data[key] = &Record{
					value:   value,
					version: 1,
				}
			}
			tableObj.version++
		}
	}

	for table, deletes := range tx.deletes {
		tbl, ok := tx.memDB.tables.Load(table)
		if !ok {
			continue
		}
		tableObj := tbl.(*Table)
		for key := range deletes {
			if _, exists := tableObj.data[key]; exists {
				delete(tableObj.data, key)
				tableObj.version++
			}
		}
	}

	tx.committed = true
	return nil
}

func (tx *Transaction) Rollback() error {
	if tx.committed {
		return errors.New("transaction already committed")
	}
	tx.committed = true
	return nil
}

func addIndexes(table *Table, model any, pk interface{}) {
	m := model.(IModel)
	for _, indexName := range m.Indexes() {
		indexVal, _ := getFieldValue(m, indexName)
		if _, ok := table.indexes[indexName]; !ok {
			table.indexes[indexName] = &Index{entries: make(map[interface{}]map[interface{}]struct{})}
		}
		index := table.indexes[indexName]
		if _, ok := index.entries[indexVal]; !ok {
			index.entries[indexVal] = make(map[interface{}]struct{})
		}
		index.entries[indexVal][pk] = struct{}{}
	}
}

func removeIndexes(table *Table, model any, pk interface{}) {
	m := model.(IModel)
	for _, indexName := range m.Indexes() {
		indexVal, _ := getFieldValue(m, indexName)
		if index, ok := table.indexes[indexName]; ok {
			if keys, ok := index.entries[indexVal]; ok {
				delete(keys, pk)
				if len(keys) == 0 {
					delete(index.entries, indexVal)
				}
			}
		}
	}
}

func getFieldValue(model any, fieldName string) (interface{}, error) {
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}
	return field.Interface(), nil
}
