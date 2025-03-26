package bizdb

import (
	"database/sql"
	"errors"
)

type MySQLLoader struct {
	db    *sql.DB
	memDB *MemoryDB
}

func NewMySQLLoader(db *sql.DB, memDB *MemoryDB) *MySQLLoader {
	return &MySQLLoader{
		db:    db,
		memDB: memDB,
	}
}

func (l *MySQLLoader) LoadTable(tableName string, pkField string) error {
	tx := l.memDB.Begin()
	defer func() {
		err := tx.Commit()
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	rows, err := l.db.Query("SELECT * FROM " + tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	pkIndex := -1
	for i, col := range cols {
		if col == pkField {
			pkIndex = i
			break
		}
	}
	if pkIndex == -1 {
		return errors.New("primary key field not found")
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		record := make(map[string]interface{})
		for i, col := range cols {
			record[col] = values[i]
		}

		if err := l.memDB.Put(tx, tableName, values[pkIndex], record); err != nil {
			return err
		}
	}

	//log.Printf("Loaded table %s with %d records", tableName, len(l.memDB.tables[tableName]))
	return nil
}
