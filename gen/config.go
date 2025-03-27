package gen

type Config struct {
	Dsn           string // 数据库连接地址
	Tables        string // 表名 默认*
	ExcludeTables string // 排除表 默认空
	ModelDir      string
	ModelPackage  string
	OnlyModel     bool
	RepoDir       string
	RepoPackage   string
}
