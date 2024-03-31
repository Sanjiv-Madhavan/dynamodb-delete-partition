package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/constants"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/handler"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/middleware"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/models"
	"github.com/spf13/cobra"
)

func init() {

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
			ctx := context.WithValue(context.Background(), constants.ClientRequestID, uuid.New())

			if tableName == "" || partitionValue == "" {
				err := errors.New("Both 'table-name' and 'partition-value' are mandatory parameters")
				middleware.LogError("Invalid tablename/partition-value", err)
				os.Exit(1)
			}

			if !skipConfirmation {
				fmt.Println("Are you sure you wanna delete the partition specified? (y/n)")

				var userConfirmation string
				_, err := fmt.Scanln(&userConfirmation)
				if err != nil {
					middleware.LogFatal("Failed to read confirmation", err)
				}

				if userConfirmation != "y" || userConfirmation != "Y" {
					middleware.LogInfo("Delete request cancelled")
					os.Exit(1)
				}
			}

			middleware.LogHandler(ctx, "Received CLI Request", constants.ClientRequestID, ctx.Value(constants.ClientRequestID).(string),
				"Table Name", tableName, "Partition Value", partitionValue)

			deleteTablePartitionInput := models.DeleteTablePartitionInput{
				TableName:      tableName,
				PartitionValue: partitionValue,
				EndpointUrl:    endpointURL,
				AwsRegion:      awsRegion,
			}

			handler, err := handler.NewTablePartitionHandler(ctx, endpointURL, awsRegion)
			if err != nil {
				middleware.LogFatal("Failed to initialise handler for deletion", err)
				os.Exit(1)
			}

			err = handler.HandleTablePartitionDeletion(ctx, deleteTablePartitionInput)
			if err != nil {
				middleware.LogHandler(ctx, "Failed to delete partition",
					constants.ClientRequestID, ":", constants.ClientRequestID, "\n err: ", err)
				middleware.LogFatal("failed to delete partition: ", err)
			}

			middleware.LogHandler(ctx, "Partition deleted successfully",
				constants.ClientRequestID, ":", constants.ClientRequestID)
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
