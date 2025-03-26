// 优化后的同步器实现
package bizdb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	maxRetries     = 3
	baseRetryDelay = 100 * time.Millisecond
)

type MySQLSynchronizer struct {
	db        *sql.DB
	memDB     *MemoryDB
	queue     chan syncTask
	stopChan  chan struct{}
	batchSize int
	interval  time.Duration
	stats     SyncStats // 同步统计
	statsMu   sync.RWMutex
	stmtCache *StmtCache // 预处理语句缓存
}

type SyncStats struct {
	EnqueueCount int64
	SuccessCount int64
	ErrorCount   int64
	LastError    error
}

type StmtCache struct {
	insertStmts *sync.Map // table -> *sql.Stmt
	updateStmts *sync.Map
	deleteStmts *sync.Map
}

type syncTask struct {
	table string
	key   interface{}
	value interface{}
	op    string // "insert", "update", "delete"
}

func NewMySQLSynchronizer(db *sql.DB, memDB *MemoryDB, batchSize int, interval time.Duration) *MySQLSynchronizer {
	return &MySQLSynchronizer{
		db:        db,
		memDB:     memDB,
		queue:     make(chan syncTask, 10000),
		stopChan:  make(chan struct{}),
		batchSize: batchSize,
		interval:  interval,
		stmtCache: &StmtCache{
			insertStmts: new(sync.Map),
			updateStmts: new(sync.Map),
			deleteStmts: new(sync.Map),
		},
	}
}

// 主要优化点1: 动态SQL生成
func generateInsertSQL(table string, value interface{}) (string, []interface{}, error) {
	// 根据具体数据结构生成
	// 示例：假设value是map
	if data, ok := value.(map[string]interface{}); ok {
		columns := make([]string, 0, len(data))
		placeholders := make([]string, 0, len(data))
		values := make([]interface{}, 0, len(data))
		for k, v := range data {
			columns = append(columns, k)
			placeholders = append(placeholders, "?")
			values = append(values, v)
		}
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table,
			strings.Join(columns, ","),
			strings.Join(placeholders, ","),
		), values, nil
	}
	return "", nil, fmt.Errorf("unsupported data type")
}

// 主要优化点2: 带重试的批处理
func (s *MySQLSynchronizer) flushBatchWithRetry(batch []syncTask) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 按操作类型分组批处理
	ops := make(map[string][]syncTask)
	for _, task := range batch {
		ops[task.op] = append(ops[task.op], task)
	}

	var wg sync.WaitGroup
	for op, tasks := range ops {
		wg.Add(1)
		go func(op string, tasks []syncTask) {
			defer wg.Done()
			for i := 0; i < maxRetries; i++ {
				err := s.processBatchOp(ctx, op, tasks)
				if err == nil {
					s.recordStats(int64(len(tasks)), nil)
					return
				}
				s.recordStats(0, err)
				time.Sleep(time.Duration(i+1) * baseRetryDelay)
			}
			log.Printf("Failed to process %d %s operations after %d retries", len(tasks), op, maxRetries)
		}(op, tasks)
	}
	wg.Wait()
}

func (s *MySQLSynchronizer) processBatchOp(ctx context.Context, op string, tasks []syncTask) error {
	if len(tasks) == 0 {
		return nil
	}

	// 按表分组提高批处理效率
	tableGroups := make(map[string][]syncTask)
	for _, task := range tasks {
		tableGroups[task.table] = append(tableGroups[task.table], task)
	}

	// 为每个表创建事务
	for table, tableTasks := range tableGroups {
		// 获取该表的预处理语句
		stmt, err := s.getStmt(table, op)
		if err != nil {
			return fmt.Errorf("prepare statement failed: %v", err)
		}

		// 开启事务
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// 批量执行
		for _, task := range tableTasks {
			switch op {
			case "insert":
				// 动态生成插入语句
				query, args, err := generateInsertSQL(table, task.value)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, query, args...); err != nil {
					return err
				}
			case "update":
				if _, err := tx.Stmt(stmt).ExecContext(ctx, task.value, task.key); err != nil {
					return err
				}
			case "delete":
				if _, err := tx.Stmt(stmt).ExecContext(ctx, task.key); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported operation: %s", op)
			}
		}

		// 提交事务
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit failed: %v", err)
		}
	}
	return nil
}

// 主要优化点3: 预处理语句缓存
func (s *MySQLSynchronizer) getStmt(table, op string) (*sql.Stmt, error) {
	var stmtMap *sync.Map
	switch op {
	case "insert":
		stmtMap = s.stmtCache.insertStmts
	case "update":
		stmtMap = s.stmtCache.updateStmts
	case "delete":
		stmtMap = s.stmtCache.deleteStmts
	default:
		return nil, fmt.Errorf("invalid operation")
	}

	if cached, ok := stmtMap.Load(table); ok {
		return cached.(*sql.Stmt), nil
	}

	// 创建新预处理语句
	var query string
	switch op {
	case "insert":
		query = fmt.Sprintf("INSERT INTO %s VALUES (?)", table) // 示例模板
	case "update":
		query = fmt.Sprintf("UPDATE %s SET value=? WHERE key=?", table)
	case "delete":
		query = fmt.Sprintf("DELETE FROM %s WHERE key=?", table)
	}

	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}

	stmtMap.Store(table, stmt)
	return stmt, nil
}

// 统计记录
func (s *MySQLSynchronizer) recordStats(count int64, err error) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()

	if err != nil {
		s.stats.ErrorCount += count
		s.stats.LastError = err
	} else {
		s.stats.SuccessCount += count
	}
	s.stats.EnqueueCount += count
}

// 在Enqueue方法中添加背压检测
func (s *MySQLSynchronizer) Enqueue(table string, key interface{}, value interface{}, op string) error {
	select {
	case s.queue <- syncTask{table, key, value, op}:
		return nil
	default:
		// 队列满时启动应急处理
		go s.emergencyFlush()
		return fmt.Errorf("queue overflow")
	}
}

// 应急快速处理
func (s *MySQLSynchronizer) emergencyFlush() {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()

	// 1. 临时扩大批处理容量
	originalBatchSize := s.batchSize
	s.batchSize *= 2
	defer func() { s.batchSize = originalBatchSize }()

	// 2. 加速处理循环
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for len(s.queue) > cap(s.queue)/2 {
		select {
		case <-ticker.C:
			s.flushCurrentBatch()
		default:
		}
	}
}

// flushCurrentBatch 处理当前队列中的任务（应急模式专用）
func (s *MySQLSynchronizer) flushCurrentBatch() {
	// 原子化获取当前队列长度
	currentQueueSize := len(s.queue)
	if currentQueueSize == 0 {
		return
	}

	// 动态计算本次处理量（不超过队列容量的1/4）
	batchCapacity := min(s.batchSize*2, cap(s.queue)/4)
	batch := make([]syncTask, 0, batchCapacity)

	// 非阻塞批量提取
	for i := 0; i < batchCapacity; i++ {
		select {
		case task := <-s.queue:
			batch = append(batch, task)
		default:
			break // 队列无更多数据
		}
	}

	if len(batch) > 0 {
		// 应急模式专用处理（快速提交+简化重试）
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 按操作类型分组后并行处理
		var wg sync.WaitGroup
		opGroups := make(map[string][]syncTask)
		for _, task := range batch {
			opGroups[task.op] = append(opGroups[task.op], task)
		}

		for op, tasks := range opGroups {
			wg.Add(1)
			go func(op string, tasks []syncTask) {
				defer wg.Done()
				_ = s.emergencyProcessOp(ctx, op, tasks) // 快速失败策略
			}(op, tasks)
		}
		wg.Wait()
	}
}

// emergencyProcessOp 应急模式处理（跳过重试直接提交）
func (s *MySQLSynchronizer) emergencyProcessOp(ctx context.Context, op string, tasks []syncTask) error {
	// 强制使用新连接避免事务阻塞
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// 原生批量SQL生成（跳过预处理）
	switch op {
	case "insert":
		batchSQL := generateBatchInsertSQL(tasks)
		if _, err := conn.ExecContext(ctx, batchSQL); err != nil {
			return err
		}
	case "update":
		for _, task := range tasks {
			sql := fmt.Sprintf("UPDATE %s SET value = ? WHERE key = ?", task.table)
			if _, err := conn.ExecContext(ctx, sql, task.value, task.key); err != nil {
				return err
			}
		}
	case "delete":
		sql := fmt.Sprintf("DELETE FROM %s WHERE key IN (?)", tasks[0].table)
		keys := extractKeys(tasks)
		if _, err := conn.ExecContext(ctx, sql, keys); err != nil {
			return err
		}
	}
	return nil
}

// 辅助函数：生成批量INSERT语句
func generateBatchInsertSQL(tasks []syncTask) string {
	if len(tasks) == 0 {
		return ""
	}

	var builder strings.Builder
	table := tasks[0].table
	builder.WriteString(fmt.Sprintf("INSERT INTO %s (key_col, value_col) VALUES ", table))

	for i, task := range tasks {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString(fmt.Sprintf("(%v, %v)", task.key, task.value))
	}
	return builder.String()
}

// 辅助函数：提取批量删除的键值
func extractKeys(tasks []syncTask) []interface{} {
	keys := make([]interface{}, len(tasks))
	for i, task := range tasks {
		keys[i] = task.key
	}
	return keys
}

// Metrics 暴露统计指标
func (s *MySQLSynchronizer) Metrics() map[string]interface{} {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	return map[string]interface{}{
		"queue_length":  len(s.queue),
		"enqueue_total": s.stats.EnqueueCount,
		"success_total": s.stats.SuccessCount,
		"error_total":   s.stats.ErrorCount,
		"last_error":    s.stats.LastError,
	}
}
