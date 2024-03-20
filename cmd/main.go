package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {

	var rootCmd = &cobra.Command{
		Use: "dynamoctl",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Hello")
		},
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
