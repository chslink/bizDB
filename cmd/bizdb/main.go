package main

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use: "bizdb",
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
		cobra.CheckErr(cmd.Help())
	},
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func main() {
	Execute()
}
