package repos_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/chslink/bizdb"
	"github.com/chslink/bizdb/test/models"
	"github.com/chslink/bizdb/test/repos"
)

func TestUserRepo(t *testing.T) {
	// 初始化内存数据库和Repository
	db := bizdb.NewMemoryDB()
	userRepo := repos.NewUserRepo(db)

	// 测试数据
	now := time.Now()
	testUser := &models.User{
		ID:        1,
		Name:      "John Doe",
		Email:     "john@example.com",
		Age:       30,
		CreatedAt: now,
	}

	t.Run("Create and Get", func(t *testing.T) {
		// 开始事务
		tx := db.Begin()

		// 创建用户
		err := userRepo.WithTx(tx).Create(testUser)
		assert.NoError(t, err, "创建用户失败")

		// 获取用户
		user, err := userRepo.WithTx(tx).GetByID(testUser.ID)
		assert.NoError(t, err, "获取用户失败")
		assert.Equal(t, testUser.ID, user.ID)
		assert.Equal(t, testUser.Name, user.Name)
		assert.Equal(t, testUser.Email, user.Email)
		assert.Equal(t, testUser.Age, user.Age)
		assert.True(t, testUser.CreatedAt.Equal(user.CreatedAt), "创建时间不匹配")

		// 提交事务
		err = tx.Commit()
		assert.NoError(t, err, "提交事务失败")

		// 提交后验证数据
		user, err = userRepo.GetByID(testUser.ID)
		assert.NoError(t, err)
		assert.Equal(t, testUser.Email, user.Email)
	})

	t.Run("GetByEmail", func(t *testing.T) {
		// 通过Email查询
		user, err := userRepo.GetByEmail(testUser.Email)
		assert.NoError(t, err)
		assert.Equal(t, testUser.ID, user.ID)

		// 查询不存在的Email
		_, err = userRepo.GetByEmail("nonexistent@example.com")
		assert.Equal(t, repos.ErrUserNotFound, err)
	})

	t.Run("Update", func(t *testing.T) {
		tx := db.Begin()
		defer tx.Rollback()

		// 更新用户信息
		updatedUser := &models.User{
			ID:        testUser.ID,
			Name:      "John Updated",
			Email:     "john.updated@example.com",
			Age:       31,
			CreatedAt: testUser.CreatedAt,
		}

		err := userRepo.WithTx(tx).Update(updatedUser)
		assert.NoError(t, err)

		// 验证更新
		user, err := userRepo.WithTx(tx).GetByID(testUser.ID)
		assert.NoError(t, err)
		assert.Equal(t, "John Updated", user.Name)
		assert.Equal(t, "john.updated@example.com", user.Email)
		assert.Equal(t, 31, user.Age)
	})

	t.Run("Delete", func(t *testing.T) {
		tx := db.Begin()

		// 删除用户
		err := userRepo.WithTx(tx).Delete(testUser.ID)
		assert.NoError(t, err)

		// 验证删除
		_, err = userRepo.WithTx(tx).GetByID(testUser.ID)
		assert.Equal(t, bizdb.ErrKeyNotFound, err)

		// 回滚删除操作
		err = tx.Rollback()
		assert.NoError(t, err)

		// 验证回滚后用户仍然存在
		user, err := userRepo.GetByID(testUser.ID)
		assert.NoError(t, err)
		assert.Equal(t, testUser.ID, user.ID)
	})

	t.Run("Query", func(t *testing.T) {
		// 添加更多测试用户
		users := []*models.User{
			{ID: 2, Name: "Alice", Email: "alice@example.com", Age: 25, CreatedAt: now},
			{ID: 3, Name: "Bob", Email: "bob@example.com", Age: 30, CreatedAt: now},
			{ID: 4, Name: "Charlie", Email: "charlie@example.com", Age: 35, CreatedAt: now},
		}

		tx := db.Begin()
		for _, u := range users {
			err := userRepo.WithTx(tx).Create(u)
			assert.NoError(t, err)
		}
		err := tx.Commit()
		assert.NoError(t, err)

		// 测试查询所有用户
		allUsers, err := userRepo.Query(repos.UserQuery{})
		assert.NoError(t, err)
		assert.Len(t, allUsers, 4, "应该查询到4个用户")

		// 测试条件查询
		age30Users, err := userRepo.Query(repos.UserQuery{Age: ptr[int](30)})
		assert.NoError(t, err)
		assert.Len(t, age30Users, 2, "应该查询到2个30岁的用户")

		// 测试分页
		pagedUsers, err := userRepo.Query(repos.UserQuery{
			Limit:  ptr[int](2),
			Offset: ptr[int](1),
		})
		assert.NoError(t, err)
		assert.Len(t, pagedUsers, 2, "分页查询应该返回2个用户")
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		const numWorkers = 10
		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for i := 0; i < numWorkers; i++ {
			go func(id int) {
				defer wg.Done()

				// 每个worker创建自己的用户
				user := &models.User{
					ID:        int64(100 + id),
					Name:      fmt.Sprintf("User%d", id),
					Email:     fmt.Sprintf("user%d@example.com", id),
					Age:       20 + id,
					CreatedAt: time.Now(),
				}

				tx := db.Begin()
				err := userRepo.WithTx(tx).Create(user)
				if err != nil {
					tx.Rollback()
					return
				}

				// 验证创建
				_, err = userRepo.WithTx(tx).GetByID(user.ID)
				if err != nil {
					tx.Rollback()
					return
				}

				err = tx.Commit()
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()

		// 验证所有用户都已创建
		allUsers, err := userRepo.Query(repos.UserQuery{})
		assert.NoError(t, err)
		assert.True(t, len(allUsers) >= numWorkers, "应该至少创建了numWorkers个用户")
	})
}

// 辅助函数，用于获取基本类型的指针
func ptr[T any](v T) *T {
	return &v
}

func TestSyncWrite100KRequests(t *testing.T) {
	// 初始化内存数据库
	db := bizdb.NewMemoryDB()
	userRepo := repos.NewUserRepo(db)

	const totalRequests = 100000

	start := time.Now()

	// 同步写入10万用户
	for i := 0; i < totalRequests; i++ {
		tx := db.Begin()

		user := &models.User{
			ID:        int64(i + 1),
			Name:      fmt.Sprintf("User%d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			Age:       20 + (i % 30),
			CreatedAt: time.Now(),
		}

		err := userRepo.WithTx(tx).Create(user)
		if err != nil {
			tx.Rollback()
			t.Fatalf("写入失败: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("提交失败: %v", err)
		}
	}

	elapsed := time.Since(start)

	t.Logf("同步写入%d次请求耗时: %v", totalRequests, elapsed)
	t.Logf("QPS: %.2f", float64(totalRequests)/elapsed.Seconds())

	// 验证写入数量
	count := 0
	tx := db.Begin()
	allUsers, err := userRepo.WithTx(tx).Query(repos.UserQuery{})
	if err != nil {
		t.Fatal("查询失败:", err)
	}
	count = len(allUsers)
	tx.Rollback()

	if count != totalRequests {
		t.Fatalf("写入数量不符，预期%d，实际%d", totalRequests, count)
	}
}

func TestAsyncWrite100KRequests(t *testing.T) {
	// 初始化内存数据库
	db := bizdb.NewMemoryDB()
	userRepo := repos.NewUserRepo(db)

	const (
		totalRequests = 100000
		workers       = 100 // 并发worker数量
	)

	var (
		wg          sync.WaitGroup
		counter     int64
		failedCount int64
	)

	start := time.Now()

	// 创建任务通道
	tasks := make(chan int, totalRequests)
	for i := 0; i < totalRequests; i++ {
		tasks <- i
	}
	close(tasks)

	// 启动worker
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range tasks {
				tx := db.Begin()

				user := &models.User{
					ID:        int64(i + 1),
					Name:      fmt.Sprintf("User%d", i),
					Email:     fmt.Sprintf("user%d@example.com", i),
					Age:       20 + (i % 30),
					CreatedAt: time.Now(),
				}

				err := userRepo.WithTx(tx).Create(user)
				if err != nil {
					tx.Rollback()
					atomic.AddInt64(&failedCount, 1)
					continue
				}

				err = tx.Commit()
				if err != nil {
					atomic.AddInt64(&failedCount, 1)
					continue
				}

				atomic.AddInt64(&counter, 1)
			}
		}()
	}

	// 等待所有worker完成
	wg.Wait()

	elapsed := time.Since(start)

	t.Logf("异步写入统计:")
	t.Logf("总请求数: %d", totalRequests)
	t.Logf("成功数: %d", counter)
	t.Logf("失败数: %d", failedCount)
	t.Logf("总耗时: %v", elapsed)
	t.Logf("QPS: %.2f", float64(counter)/elapsed.Seconds())

	// 验证写入数量
	count := 0
	tx := db.Begin()
	allUsers, err := userRepo.WithTx(tx).Query(repos.UserQuery{})
	if err != nil {
		t.Fatal("查询失败:", err)
	}
	count = len(allUsers)
	tx.Rollback()

	if count != int(counter) {
		t.Fatalf("写入数量不符，预期%d，实际%d", counter, count)
	}
}
