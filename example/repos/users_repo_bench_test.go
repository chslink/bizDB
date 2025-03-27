package repos_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/chslink/bizdb"
	"github.com/chslink/bizdb/example/models"
	"github.com/chslink/bizdb/example/repos"
)

// 初始化测试数据
func setupBenchmark(b *testing.B) (*bizdb.MemoryDB, *repos.UsersRepo) {
	db := bizdb.NewMemoryDB()
	repo := repos.NewUsersRepo(db)

	// 预加载1000个用户作为测试数据
	tx := db.Begin()
	for i := 0; i < 1000; i++ {
		user := &models.Users{
			Id:       int64(i + 1),
			Name:     "User",
			Email:    "user@example.com",
			Age:      int64(20 + (i % 30)),
			CreateAt: time.Now(),
		}
		_ = repo.WithTx(tx).Create(user)
	}
	_ = tx.Commit()

	return db, repo
}

// BenchmarkCreateUser 测试创建用户性能
func BenchmarkCreateUser(b *testing.B) {
	db, repo := setupBenchmark(b)
	defer func() {
		// 清理数据
		tx := db.Begin()
		for i := 0; i < b.N; i++ {
			_ = repo.WithTx(tx).Delete(int64(i + 1001))
		}
		_ = tx.Commit()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.Begin()
		user := &models.Users{
			Id:       int64(i + 1001),
			Name:     "Benchmark User",
			Email:    "benchmark@example.com",
			Age:      30,
			CreateAt: time.Now(),
		}
		_ = repo.WithTx(tx).Create(user)
		_ = tx.Commit()
	}
}

// BenchmarkGetUserByID 测试通过ID获取用户性能
func BenchmarkGetUserByID(b *testing.B) {
	_, repo := setupBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = repo.GetById(int64(i%1000 + 1))
	}
}

// BenchmarkGetUserByEmail 测试通过Email获取用户性能
func BenchmarkGetUserByEmail(b *testing.B) {
	_, repo := setupBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = repo.GetByEmail("user@example.com")
	}
}

// BenchmarkUpdateUser 测试更新用户性能
func BenchmarkUpdateUser(b *testing.B) {
	db, repo := setupBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.Begin()
		user := &models.Users{
			Id:       int64(i%1000 + 1),
			Name:     "Updated User",
			Email:    "updated@example.com",
			Age:      35,
			CreateAt: time.Now(),
		}
		_ = repo.WithTx(tx).Update(user)
		_ = tx.Commit()
	}
}

// BenchmarkQueryUsers 测试查询用户性能
func BenchmarkQueryUsers(b *testing.B) {
	_, repo := setupBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = repo.Query(repos.UsersQuery{
			Age:   ptr[int64](25),
			Limit: ptr[int](100),
		})
	}
}

// BenchmarkConcurrentOperations 测试并发操作性能
func BenchmarkConcurrentOperations(b *testing.B) {
	db, repo := setupBenchmark(b)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 混合读写操作
			tx := db.Begin()

			// 读取
			_, _ = repo.WithTx(tx).GetById(1)

			// 写入
			_ = repo.WithTx(tx).Update(&models.Users{
				Id:    1,
				Name:  "Concurrent Update",
				Email: "concurrent@example.com",
				Age:   40,
			})

			// 查询
			_, _ = repo.WithTx(tx).Query(repos.UsersQuery{Age: ptr[int64](30)})

			_ = tx.Commit()
		}
	})
}

// BenchmarkLargeDataset 测试大规模数据集下的性能
func BenchmarkLargeDataset(b *testing.B) {
	db := bizdb.NewMemoryDB()
	repo := repos.NewUsersRepo(db)

	// 预加载10万用户
	b.Run("Setup", func(b *testing.B) {
		tx := db.Begin()
		for i := 0; i < 100000; i++ {
			user := &models.Users{
				Id:       int64(i + 1),
				Name:     "User",
				Email:    "user@example.com",
				Age:      int64(20 + (i % 30)),
				CreateAt: time.Now(),
			}
			_ = repo.WithTx(tx).Create(user)
		}
		_ = tx.Commit()
	})

	// 测试大规模数据查询
	b.Run("QueryLargeDataset", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = repo.Query(repos.UsersQuery{
				Age:   ptr[int64](25),
				Limit: ptr[int](1000),
			})
		}
	})

	// 测试大规模数据分页
	b.Run("PaginationLargeDataset", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			page := (i % 100) + 1
			_, _ = repo.Query(repos.UsersQuery{
				Limit:  ptr[int](100),
				Offset: ptr[int]((page - 1) * 100),
			})
		}
	})
}

// BenchmarkTransaction 测试事务性能
func BenchmarkTransaction(b *testing.B) {
	db, repo := setupBenchmark(b)

	b.Run("SingleOperation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := db.Begin()
			_ = repo.WithTx(tx).Delete(1)
			_ = tx.Commit()
		}
	})

	b.Run("MultipleOperations", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := db.Begin()
			_ = repo.WithTx(tx).Create(&models.Users{
				Id:       int64(1000 + i + 1),
				Name:     "New User",
				Email:    "new@example.com",
				Age:      30,
				CreateAt: time.Now(),
			})
			_ = repo.WithTx(tx).Update(&models.Users{
				Id:    1,
				Name:  "Updated",
				Email: "updated@example.com",
				Age:   35,
			})
			_, _ = repo.WithTx(tx).Query(repos.UsersQuery{})
			_ = tx.Commit()
		}
	})
}

// BenchmarkTransactionQPS 测试纯事务操作的QPS
func BenchmarkTransactionQPS(b *testing.B) {
	db := bizdb.NewMemoryDB()
	userRepo := repos.NewUsersRepo(db)

	// 预加载测试用户
	user := &models.Users{
		Id:       1,
		Name:     "QPS Test",
		Email:    "qps@test.com",
		Age:      30,
		CreateAt: time.Now(),
	}
	tx := db.Begin()
	_ = userRepo.WithTx(tx).Create(user)
	_ = tx.Commit()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx := db.Begin()
			// 读取-修改-写入事务
			u, _ := userRepo.WithTx(tx).GetById(1)
			u.Age++
			_ = userRepo.WithTx(tx).Update(u)
			_ = tx.Commit()
		}
	})
}

// BenchmarkTransactionQPS_WriteOnly 测试纯写入事务QPS
func BenchmarkTransactionQPS_WriteOnly(b *testing.B) {
	db := bizdb.NewMemoryDB()
	userRepo := repos.NewUsersRepo(db)

	var counter int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddInt64(&counter, 1)
			tx := db.Begin()
			user := &models.Users{
				Id:       id,
				Name:     "Write Test",
				Email:    "write@test.com",
				Age:      30,
				CreateAt: time.Now(),
			}
			_ = userRepo.WithTx(tx).Create(user)
			_ = tx.Commit()
		}
	})
}
