package gen

import (
	"testing"

	"gopkg.in/yaml.v3"
)

var userModelDef = `
name: User
table: users
fields:
  - name: ID
    type: int64
    primary: true
  - name: Name
    type: string
    size: 100
  - name: Email
    type: string
    size: 255
    unique: true
  - name: Age
    type: int
  - name: CreatedAt
    type: time.Time`

func TestTool(t *testing.T) {
	model := Model{}
	err := yaml.Unmarshal([]byte(userModelDef), &model)
	if err != nil {
		t.Fatal(err)
	}
	err = GenerateCode(model, "./test/models")
	if err != nil {
		t.Fatal(err)
	}
}
