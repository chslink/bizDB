package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/chslink/bizdb/gen"
)

var (
	dsn           string
	modelsDir     string
	modelsPkg     string
	onlyModel     bool
	repoDir       string
	repoPkg       string
	tables        string
	excludeTables string
	config        string
	importPath    string
)

func init() {
	rootCmd.AddCommand(genCmd)

	genCmd.PersistentFlags().StringVarP(&config, "config", "c", "", "config file path")
	genCmd.PersistentFlags().StringVar(&dsn, "dsn", "", "mysql connect dsn (user:password@tcp(localhost:3306)/your_database)")
	genCmd.PersistentFlags().StringVar(&modelsDir, "model_dir", "./models", "models output dir")
	genCmd.PersistentFlags().StringVar(&modelsPkg, "model_package", "models", "models output pkg")
	genCmd.PersistentFlags().BoolVar(&onlyModel, "only_model", false, "only generate model")
	genCmd.PersistentFlags().StringVar(&repoDir, "repo_dir", "./repos", "repositories output dir")
	genCmd.PersistentFlags().StringVar(&repoPkg, "repo_package", "repos", "repositories output pkg")
	genCmd.PersistentFlags().StringVar(&tables, "tables", "*", "tables to generate (biz*  biz[1-2]  biz??? )")
	genCmd.PersistentFlags().StringVar(&excludeTables, "exclude_tables", "", "exclude tables to generate")
	genCmd.PersistentFlags().StringVarP(&importPath, "model_import_path", "i", "./models", "use for generate repo codes,import your models import path,")

}

var genCmd = &cobra.Command{
	Use:   "gen",
	Short: "Generate code",
	Long:  `Generate code`,
	Run: func(cmd *cobra.Command, args []string) {
		if config != "" {
			viper.SetConfigFile(config)
			err := viper.ReadInConfig()
			if err != nil {
				cobra.CheckErr(err)
			}
			conf := &gen.Config{}
			conf.Dsn = viper.GetString("dsn")
			conf.ModelDir = viper.GetString("model_dir")
			conf.ModelPackage = viper.GetString("model_package")
			conf.OnlyModel = viper.GetBool("only_model")
			conf.RepoDir = viper.GetString("repo_dir")
			conf.RepoPackage = viper.GetString("repo_package")
			conf.Tables = viper.GetString("tables")
			conf.ExcludeTables = viper.GetString("exclude_tables")
			conf.ModelImportPath = viper.GetString("model_import_path")
			cobra.CheckErr(gen.Run(conf))
			return
		} else {
			if dsn == "" {
				cobra.CheckErr(cmd.Help())
				return
			}
			err := gen.Run(&gen.Config{
				Dsn:             dsn,
				ModelDir:        modelsDir,
				ModelPackage:    modelsPkg,
				OnlyModel:       onlyModel,
				RepoDir:         repoDir,
				RepoPackage:     repoPkg,
				Tables:          tables,
				ExcludeTables:   excludeTables,
				ModelImportPath: importPath,
			})
			cobra.CheckErr(err)
		}

	},
}
