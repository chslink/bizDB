package bizdb

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

var (
	ErrTransactionNotFound = errors.New("transaction not found")
	ErrKeyNotFound         = errors.New("key not found")
	ErrWriteConflict       = errors.New("write conflict detected")
	ErrTableNotFound       = errors.New("table not found")
)

type Record struct {
	value   any
	version int64
}

type Index struct {
	Name   string
	Fields []string
	Unique bool
	Type   string
}

type IModel interface {
	TableName() string
	Indexes() []Index
}

type Copyable[T any] interface {
	Copy() T
}

type Table struct {
	mu      sync.RWMutex
	data    map[interface{}]*Record
	version int64
}

type Transaction struct {
	id           int64
	memDB        *MemoryDB
	readCache    map[string]map[interface{}]interface{}
	keySnapshots map[string]map[interface{}]int64
	updates      map[string]map[interface{}]any
	deletes      map[string]map[interface{}]struct{}
	committed    bool
}

type MemoryDB struct {
	tables    sync.Map
	txCounter int64
	activeTx  sync.Map
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
		readCache:    make(map[string]map[interface{}]interface{}),
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
			return nil, ErrTableNotFound
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

	if _, ok := db.activeTx.Load(tx.id); !ok {
		return nil, ErrTransactionNotFound
	}

	if tx.committed {
		return nil, errors.New("transaction already committed")
	}

	if tableCache, ok := tx.readCache[table]; ok {
		if val, exists := tableCache[key]; exists {
			return val, nil
		}
	}

	if deletes, ok := tx.deletes[table]; ok {
		if _, deleted := deletes[key]; deleted {
			return nil, ErrKeyNotFound
		}
	}

	if updates, ok := tx.updates[table]; ok {
		if val, updated := updates[key]; updated {
			return val, nil
		}
	}

	tbl, ok := db.tables.Load(table)
	if !ok {
		return nil, ErrTableNotFound
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

	if tx.readCache[table] == nil {
		tx.readCache[table] = make(map[interface{}]interface{})
	}
	tx.readCache[table][key] = record.value
	return record.value, nil
}

func (db *MemoryDB) Put(tx *Transaction, table string, key interface{}, value any) error {
	if tx == nil {
		return errors.New("write operation requires a transaction")
	}

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

func (db *MemoryDB) Range(tx *Transaction, tableName string, f func(id, val any) bool) error {
	if tx == nil {
		tbl, ok := db.tables.Load(tableName)
		if !ok {
			return ErrTableNotFound
		}
		tableObj := tbl.(*Table)
		tableObj.mu.RLock()
		defer tableObj.mu.RUnlock()

		for k, v := range tableObj.data {
			if !f(k, v.value) {
				return nil
			}
		}
		return nil
	}

	if tx.committed {
		return errors.New("transaction already committed")
	}

	// 处理事务中的更新
	if updates, ok := tx.updates[tableName]; ok {
		for k, v := range updates {
			if deletes, ok := tx.deletes[tableName]; ok {
				if _, deleted := deletes[k]; deleted {
					continue
				}
			}
			if !f(k, v) {
				return nil
			}
		}
	}

	// 处理基础表中的数据，排除被删除或已更新的
	tbl, ok := db.tables.Load(tableName)
	if !ok {
		return nil
	}
	tableObj := tbl.(*Table)
	tableObj.mu.RLock()
	defer tableObj.mu.RUnlock()

	for k, record := range tableObj.data {
		if deletes, ok := tx.deletes[tableName]; ok {
			if _, deleted := deletes[k]; deleted {
				continue
			}
		}
		if updates, ok := tx.updates[tableName]; ok {
			if _, updated := updates[k]; updated {
				continue
			}
		}
		if !f(k, record.value) {
			return nil
		}
	}

	return nil
}

func (tx *Transaction) Commit() error {
	defer tx.memDB.activeTx.Delete(tx.id)

	if tx.committed {
		return errors.New("transaction already committed")
	}

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

	tableNames := make([]string, 0, len(tables))
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
	tx.memDB.activeTx.Delete(tx.id)
	return nil
}
