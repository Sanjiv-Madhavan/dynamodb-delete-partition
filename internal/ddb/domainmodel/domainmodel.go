package domainmodel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/client"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/constants"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/models"
)

type DomainModel struct {
	client *dynamodb.Client
}

func NewTablePartitionService(ctx context.Context, endpointURL string, awsRegion string) (*DomainModel, error) {

	client, err := client.NewDynamoDBClient(ctx, endpointURL, awsRegion)

	if err != nil {
		slog.Info("Got error while initializing DynamoDB connection",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			constants.LogErrorKey, err)
		return nil, err
	}

	return &DomainModel{
		client: client,
	}, nil
}

func (d *DomainModel) DeletePartition(ctx context.Context, dtpi models.DeleteTablePartitionInput) error {
	slog.Info("Delete Table Partition request recieved",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"layer", "domainmodel",
		"table-name", dtpi.TableName,
		"partition-value", dtpi.PartitionValue,
		"endpoint-url", dtpi.EndpointUrl,
		"region", dtpi.AwsRegion)

	_, err := d.checkTableExists(ctx, dtpi)
	if err != nil {
		return err
	}

	// Inititate Delete
	// Use func NewQueryPaginator(client QueryAPIClient, params *QueryInput, optFns ...func(*QueryPaginatorOptions)) *QueryPaginator
	paginator, err := d.CreateQueryPaginator(ctx, dtpi)
	if err != nil {
		return err
	}
	// Create Iterable Query Paginator
	d.batchWritePagination(ctx, paginator)

	return nil
}

func (d *DomainModel) batchWritePagination(ctx context.Context, paginator *dynamodb.QueryPaginator) error {

	batchWriteInputStream := make(chan dynamodb.BatchWriteItemInput, 2)
	batchWriteOutputStream := make(chan dynamodb.BatchWriteItemOutput, 2)

	var wgDeleteItemsInBatch sync.WaitGroup
	wgDeleteItemsInBatch.Add(1)

	slog.Info("Goroutine - batchDeleteTablePageItems - batchWriteInputStream - batchWriteOutputStream - Triggered",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))

	go d.batchDeleteTablePageItems(ctx, &wgDeleteItemsInBatch, batchWriteInputStream, batchWriteOutputStream)

	var wgDeletePartitionSummary sync.WaitGroup
	wgDeletePartitionSummary.Add(1)
	slog.Info("Goroutine - logDeleteTablePartitionSummary - batchWriteItemOutputStream - Triggered",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
	go d.logDeleteTablePartitionSummary(ctx, &wgDeletePartitionSummary, batchWriteOutputStream)

	var wg sync.WaitGroup

	for paginator.HasMorePages() {
		slog.Info("In paginator hasmore")
		queryOutput, err := paginator.NextPage(ctx)
		slog.Info("Number of items for page found",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			"Length", len(queryOutput.Items))
		if err != nil {
			return fmt.Errorf("failed to iterate over next pages, here it is why - %s", err)
		}
		wg.Add(1)
		slog.Info("Goroutine - deleteTablePageItems - Triggered",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
		go d.deleteTablePageItems(ctx, &wg, batchWriteInputStream, queryOutput)
	}
	slog.Info("Waiting for - Goroutine - deleteTablePageItems - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Waiting")
	wg.Wait()
	slog.Info("Waiting for - Goroutine - deleteTablePageItems - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Done")
	slog.Info("Closing batchWriteItemInputStream - channel",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
	close(batchWriteInputStream)

	slog.Info("Waiting for - batchDeleteTablePageItems - DeleteRquest - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Waiting")
	wgDeleteItemsInBatch.Wait()
	slog.Info("Waiting for - batchDeleteTablePageItems - DeleteRquest - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Done")

	slog.Info("Closing batchWriteItemOutputStream - channel",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
	close(batchWriteOutputStream)

	slog.Info("Waiting for - logDeleteTablePartitionSummary - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Waiting")
	wgDeletePartitionSummary.Wait()
	slog.Info("Waiting for - logDeleteTablePartitionSummary - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Done")
	return nil

}

func (d *DomainModel) deleteTablePageItems(ctx context.Context, wg *sync.WaitGroup,
	batchWriteInputStream chan<- dynamodb.BatchWriteItemInput, queryOutput *dynamodb.QueryOutput) {

	batchWriteItemInputs := d.prepareBatchWriteItemInputs(ctx, queryOutput)
	slog.Info("Total number of batches for paginator page",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Length", len(batchWriteItemInputs))

	for _, batchWriteItemInput := range batchWriteItemInputs {
		slog.Debug("Streaming table page batch for deletetion",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			"layer", "domainmodel",
			"method", "deleteTablePageBatchItems")
		batchWriteInputStream <- batchWriteItemInput
	}
	wg.Done()
}

func (d *DomainModel) prepareBatchWriteItemInputs(ctx context.Context, queryOutput *dynamodb.QueryOutput) []dynamodb.BatchWriteItemInput {
	tableName := aws.ToString(queryOutput.ConsumedCapacity.TableName)
	slog.Info("Table Name from page object",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"TableName", tableName)
	var deleteRequests []types.WriteRequest

	for _, item := range queryOutput.Items {
		deleteRequest := types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: item,
			},
		}
		deleteRequests = append(deleteRequests, deleteRequest)
	}

	var batches []dynamodb.BatchWriteItemInput
	deleteRequestLen := len(deleteRequests)
	for i := 0; i < deleteRequestLen; i += constants.BatchWriteItemSize {
		end := i + constants.BatchWriteItemSize
		if end > deleteRequestLen {
			end = deleteRequestLen
		}
		batches = append(batches, dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: deleteRequests[i:end],
			},
			ReturnConsumedCapacity:      types.ReturnConsumedCapacityTotal,
			ReturnItemCollectionMetrics: types.ReturnItemCollectionMetricsSize,
		})
	}
	return batches
}

func (d *DomainModel) batchDeleteTablePageItems(ctx context.Context, wgDeleteItemsInBatch *sync.WaitGroup,
	batchWriteInputStream <-chan dynamodb.BatchWriteItemInput,
	batchWriteOutputStream chan<- dynamodb.BatchWriteItemOutput) {
	for batchWriteItemInput := range batchWriteInputStream {
		slog.Debug("Received table page batch delete request",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			"layer", "domainmodel",
			"method", "deleteTablePageBatchItems")
		batchWriteItemOutput, err := d.client.BatchWriteItem(ctx, &batchWriteItemInput)
		if err != nil {
			slog.Error("Failed to delete items batch",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
				"error", err)
			return
		}
		batchWriteOutputStream <- *batchWriteItemOutput
	}
	wgDeleteItemsInBatch.Done()
}

func (d *DomainModel) logDeleteTablePartitionSummary(ctx context.Context, wgDeletePartitionSummary *sync.WaitGroup,
	batchWriteOutputStream <-chan dynamodb.BatchWriteItemOutput) {

	totalItemsCount := 0
	totalCapacityUnits := 0.0
	for batchWriteItemOutput := range batchWriteOutputStream {
		slog.Debug("logDeleteTablePartitionSummary - request - received",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
		for key := range batchWriteItemOutput.ItemCollectionMetrics {
			totalItemsCount += len(batchWriteItemOutput.ItemCollectionMetrics[key])
		}
		for _, consumedCapacity := range batchWriteItemOutput.ConsumedCapacity {
			totalCapacityUnits += aws.ToFloat64(consumedCapacity.CapacityUnits)
		}
	}
	slog.Info("Delete Partition Summary Report",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"TotalDeletedItems", totalItemsCount,
		"TotalConsumedCapacityUnits", totalCapacityUnits)
	wgDeletePartitionSummary.Done()
}

func (d *DomainModel) checkTableExists(ctx context.Context, dtpi models.DeleteTablePartitionInput) (bool, error) {

	var exists = true
	slog.Info("Checking table exists")
	tableName, err := d.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(dtpi.TableName),
	})

	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			slog.Error("Table does not exists",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
				"Table", tableName,
				constants.LogErrorKey, err)
		} else {
			slog.Error("Couldn't determine existence of table",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
				"Table", tableName,
				constants.LogErrorKey, err)
		}
		exists = false
	}

	return exists, nil
}

func (d *DomainModel) CreateQueryPaginator(ctx context.Context, dtpi models.DeleteTablePartitionInput) (*dynamodb.QueryPaginator, error) {
	// Create QueryInput
	queryInput, err := d.CreateQueryInput(ctx, dtpi.TableName, dtpi.PartitionValue)
	if err != nil {
		return nil, err
	}

	paginator := dynamodb.NewQueryPaginator(d.client, queryInput)
	return paginator, nil
}

func (d *DomainModel) CreateQueryInput(ctx context.Context, tableName string, partitionValue string) (*dynamodb.QueryInput, error) {
	schema, err := d.CreateTableKeySchema(ctx, tableName)
	if err != nil {
		return nil, err
	}

	// mock-query:
	// aws dynamodb get-item \
	//  --table-name ProductCatalog \
	//  --key '{"Id":{"N":"123"}}' \
	//  --projection-expression "#c" \
	//  --expression-attribute-names '{"#c":"Comment"}'

	// Below Query fetches the partition key in the table and u
	// ses Expression attr values to match it with partitionValue given in cli
	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		ProjectionExpression:   aws.String(schema.PartitionKey),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :%s", schema.PartitionKey, schema.PartitionKey)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			fmt.Sprintf(":%s", schema.PartitionKey): &types.AttributeValueMemberS{
				Value: partitionValue,
			},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	}

	if schema.RangeKey != "" {
		queryInput.ProjectionExpression = aws.String(fmt.Sprintf("%s, #sortkey", schema.PartitionKey))
		queryInput.ExpressionAttributeNames = map[string]string{"#sortkey": schema.RangeKey}
	}

	return queryInput, nil
}

func (d *DomainModel) CreateTableKeySchema(ctx context.Context, tableName string) (*models.TableKeySchema, error) {

	tableOutput, err := d.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		slog.Error("Couldn't found the Table information, here it is why",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			"Table", tableName,
			constants.LogErrorKey, err)
		return nil, err
	}
	if tableOutput == nil {
		return nil, fmt.Errorf("dynamodb table `%s` does not exists", tableName)
	}

	schema := &models.TableKeySchema{
		TableName: tableName,
	}
	keySchemaElements := tableOutput.Table.KeySchema
	for _, keySchemaElement := range keySchemaElements {
		if keySchemaElement.KeyType == types.KeyTypeHash {
			schema.PartitionKey = aws.ToString(keySchemaElement.AttributeName)
		} else if keySchemaElement.KeyType == types.KeyTypeRange {
			schema.RangeKey = aws.ToString(keySchemaElement.AttributeName)
		}
		if schema.PartitionKey != "" && schema.RangeKey != "" {
			break
		}
	}

	return schema, nil

}
