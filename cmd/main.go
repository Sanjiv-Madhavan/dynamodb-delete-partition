package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/google/uuid"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/constants"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/handler"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/middleware"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/models"
	"github.com/spf13/cobra"
)

func init() {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	logger := slog.New(handler)
	slog.SetDefault(logger)
}

func main() {

	middleware := middleware.NewMiddleware()

	var tableName, partitionValue, endpointURL, awsRegion string
	var skipConfirmation bool

	var rootCmd = &cobra.Command{
		Use: "dynamoctl",
	}

	var deleteCmd = &cobra.Command{
		Use:   "delete-partition",
		Short: "Delete a specific partition from a dynamoDB table",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.WithValue(context.Background(), constants.CliRequestId, uuid.New())
			if tableName == "" || partitionValue == "" {
				slog.Info("Both 'table-name' and 'partition-value' are mandatory parameters.",
					constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
				os.Exit(1)
			}

			if !skipConfirmation {
				fmt.Println("Are you sure you wanna delete the partition specified? (y/n)")

				var userConfirmation string
				_, err := fmt.Scanln(&userConfirmation)
				if err != nil {
					slog.Error("Failed to read user input for confirmation.",
						constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
						"error", err)
					os.Exit(1)
				}

				middleware.LogHandler(ctx, "Logging User Input: ", userConfirmation)

				if userConfirmation != "y" && userConfirmation != "Y" {
					slog.Info("delete-partition request canceled.",
						constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
					return
				}
			}

			slog.Info("ddbctl delete-partition cli parameters",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
				"table-name", tableName,
				"partition-value", partitionValue,
				"endpoint-url", endpointURL,
				"region", awsRegion)

			deleteTablePartitionInput := models.DeleteTablePartitionInput{
				TableName:      tableName,
				PartitionValue: partitionValue,
				EndpointUrl:    endpointURL,
				AwsRegion:      awsRegion,
			}

			handler, err := handler.NewTablePartitionHandler(ctx, endpointURL, awsRegion)
			if err != nil {
				slog.Error("Error while initializing Handler object",
					constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
					"error", err)
				os.Exit(1)
			}

			err = handler.HandleTablePartitionDeletion(ctx, deleteTablePartitionInput)
			if err != nil {
				slog.Error("Failed to delete-partition",
					constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
					"error", err)
				os.Exit(1)
			}

			slog.Info("delete-partition request completed successfully.",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
		},
	}

	deleteCmd.Flags().StringVarP(&tableName, "table-name", "t", "", "Name of the DynamoDB table (mandatory)")
	deleteCmd.Flags().StringVarP(&partitionValue, "partition-value", "p", "", "Value of the partition key (mandatory)")
	deleteCmd.Flags().StringVarP(&endpointURL, "endpoint-url", "e", "http://localhost:8000", "Endpoint URL - http://localhost:8000")
	deleteCmd.Flags().StringVarP(&awsRegion, "region", "r", "us-east-1", "AWS region (optional)")
	deleteCmd.Flags().BoolVarP(&skipConfirmation, "skip-confirmation", "s", false, "Skip confirmation prompt")

	deleteCmd.MarkFlagRequired("table-name")
	deleteCmd.MarkFlagRequired("partition-value")

	rootCmd.AddCommand(deleteCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
