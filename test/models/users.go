package models

import (
	"time"
)

type User struct {
	ID        int64
	Name      string
	Email     string
	Age       int
	CreatedAt time.Time
}

func (m *User) TableName() string {
	return "users"
}

func (m *User) Indexes() []string {
	return []string{"id", "email"}
}

// Copy 创建结构体的深拷贝
func (m *User) Copy() *User {
	if m == nil {
		return nil
	}

	cpy := &User{
		ID:        m.ID,
		Name:      m.Name,
		Email:     m.Email,
		Age:       m.Age,
		CreatedAt: m.CreatedAt,
	}

	return cpy
}
