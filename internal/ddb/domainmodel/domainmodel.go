package domainmodel

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/client"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/constants"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/logger"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/middleware"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/models"
	"go.uber.org/zap"
)

type DomainModel struct {
	client *dynamodb.Client
	log    *logger.Logger
}

func NewTablePartitionService(ctx context.Context, endpointURL string, awsRegion string) (*DomainModel, error) {

	client, err := client.NewDynamoDBClient(ctx, endpointURL, awsRegion)
	middleware := middleware.NewMiddleware()
	if err != nil {
		middleware.LogError("Got error while initializing DynamoDB connection", err)
		return nil, err
	}

	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{"stdout"}

	zaplog, err := cfg.Build()
	if err != nil {
		panic("failed to instantiate zap logger")
	}

	zaplog.Info("Logging to stdout with Zap logger")

	log := logger.NewLogger(zaplog, middleware)
	return &DomainModel{
		client: client,
		log:    log,
	}, nil
}

func (d *DomainModel) DeletePartition(ctx context.Context, dtpi models.DeleteTablePartitionInput) error {
	d.log.Middleware.LogHandler(ctx, ctx.Value(constants.ClientRequestID).(string), "Deleting table with below details \n",
		"Table Name: ", dtpi.TableName, "\n",
		"Partition key: ", dtpi.PartitionValue, "\n",
		"Region: ", dtpi.AwsRegion, "\n",
		"Endpoint: ", dtpi.EndpointUrl, "\n")

	_, err := d.checkTableExists(ctx, dtpi)
	if err != nil {
		return err
	}

	// Inititate Delete
	// Use func NewQueryPaginator(client QueryAPIClient, params *QueryInput, optFns ...func(*QueryPaginatorOptions)) *QueryPaginator
	paginator, err := d.CreateQueryPaginator(ctx, dtpi)
	if err != nil {
		d.log.Middleware.LogError("Unable to delete partition", err)
	}
	// Create Iterable Query Paginator
	d.batchWritePagination(ctx, paginator)

	return nil
}

func (d *DomainModel) batchWritePagination(ctx context.Context, paginator *dynamodb.QueryPaginator) error {

	batchWriteInputStream := make(chan dynamodb.BatchWriteItemInput, 3)
	batchWriteOutputStream := make(chan dynamodb.BatchWriteItemOutput, 3)

	var wgDeleteItemsInBatch sync.WaitGroup
	wgDeleteItemsInBatch.Add(1)

	d.log.Middleware.LogHandler(ctx, "Go routine Delte Items in batch starting. Waiting for the BatchWriteInputStream to receive values",
		constants.ClientRequestID, ": ", ctx.Value(constants.ClientRequestID))

	go d.batchDeleteTablePageItems(ctx, &wgDeleteItemsInBatch, batchWriteInputStream, batchWriteOutputStream)

	var wgDeletePartitionSummary sync.WaitGroup
	wgDeletePartitionSummary.Add(1)
	d.log.Middleware.LogHandler(ctx, "Goroutine - logDeleteTablePartitionSummary - batchWriteItemOutputStream - Triggered",
		constants.ClientRequestID, ": ", ctx.Value(constants.ClientRequestID))
	go d.logDeleteTablePartitionSummary(ctx, &wgDeletePartitionSummary, batchWriteOutputStream)

	var wg sync.WaitGroup
	for paginator.HasMorePages() {
		queryOutput, err := paginator.NextPage(ctx)
		if err != nil {
			d.log.Middleware.LogError("Error while iterating pages", err)
		}
		wg.Add(1)
		d.log.Middleware.LogHandler(ctx, "Page has these many items: ", len(queryOutput.Items))
		go d.deleteTablePageItems(ctx, &wg, batchWriteInputStream, queryOutput)
	}
	return nil

}

func (d *DomainModel) deleteTablePageItems(ctx context.Context, wg *sync.WaitGroup,
	batchWriteInputStream chan<- dynamodb.BatchWriteItemInput, queryOutput *dynamodb.QueryOutput) {

	batchWriteItemInputs := d.prepareBatchWriteItemInputs(ctx, queryOutput)
	d.log.Middleware.LogHandler(ctx,
		constants.ClientRequestID, ":", ctx.Value(constants.ClientRequestID),
		"Total batches d=for the write operation is: ", len(batchWriteItemInputs))

	for _, batchWriteItemInput := range batchWriteItemInputs {
		d.log.Log.Debug("Streaming batch items for delete",
			zap.Any(constants.ClientRequestID, ctx.Value(constants.ClientRequestID)))
		batchWriteInputStream <- batchWriteItemInput
	}
	wg.Done()
}

func (d *DomainModel) prepareBatchWriteItemInputs(ctx context.Context, queryOutput *dynamodb.QueryOutput) []dynamodb.BatchWriteItemInput {
	tableName := aws.ToString(queryOutput.ConsumedCapacity.TableName)
	d.log.Middleware.LogHandler(ctx, "Table name from page object query output", tableName)
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
		d.log.Middleware.LogHandler(ctx, "Received table page batch delete request",
			constants.ClientRequestID, ":", ctx.Value(constants.ClientRequestID),
			"layer", "DomainModel",
			"method", "deleteTablePageBatchItems")
		batchWriteItemOutput, err := d.client.BatchWriteItem(ctx, &batchWriteItemInput)
		if err != nil {
			d.log.Middleware.LogError("Error while deleting table items in layer domainModel", err)
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
		d.log.Middleware.LogHandler(ctx, "logDeleteTablePartitionSummary - request - received",
			constants.ClientRequestID, ":", ctx.Value(constants.ClientRequestID),
		)
		for key := range batchWriteItemOutput.ItemCollectionMetrics {
			totalItemsCount += len(batchWriteItemOutput.ItemCollectionMetrics[key])
		}
		for _, consumedCapacity := range batchWriteItemOutput.ConsumedCapacity {
			totalCapacityUnits += aws.ToFloat64(consumedCapacity.CapacityUnits)
		}
	}
	d.log.Middleware.LogHandler(ctx, "Delete Partition Summary Report",
		constants.ClientRequestID, ":", ctx.Value(constants.ClientRequestID),
		"TotalDeletedItems", totalItemsCount,
		"TotalConsumedCapacityUnits", totalCapacityUnits)
	wgDeletePartitionSummary.Done()
}

func (d *DomainModel) checkTableExists(ctx context.Context, dtpi models.DeleteTablePartitionInput) (bool, error) {

	tableName, err := d.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(dtpi.TableName),
	})

	if err != nil {
		var ResourceNotFoundException *types.ResourceNotFoundException
		d.log.Middleware.LogError(fmt.Sprintf("unable to find table: %s. Please provide a valid tablename", *tableName.Table.TableName), ResourceNotFoundException)
		return false, err
		//add request id to each logs
	}

	return true, nil
}

func (d *DomainModel) CreateQueryPaginator(ctx context.Context, dtpi models.DeleteTablePartitionInput) (*dynamodb.QueryPaginator, error) {
	// Create QueryInput
	queryInput, err := d.CreateQueryInput(ctx, dtpi.TableName, dtpi.PartitionValue)
	if err != nil {
		d.log.Middleware.LogError("Unable to create Query Inout", err)
		return nil, err
	}

	paginator := dynamodb.NewQueryPaginator(d.client, queryInput)
	return paginator, nil
}

func (d *DomainModel) CreateQueryInput(ctx context.Context, tableName string, partitionValue string) (*dynamodb.QueryInput, error) {
	schema, err := d.CreateTableKeySchema(ctx, tableName)
	if err != nil {
		d.log.Middleware.LogError("error while creating schema", err)
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
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = %s", schema.PartitionKey, schema.PartitionKey)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			fmt.Sprintf(":%s", schema.PartitionKey): &types.AttributeValueMemberS{
				Value: partitionValue,
			},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	}

	fmt.Println(queryInput) // Delete
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
		d.log.Middleware.LogError("Unable to find resource", err)
		return nil, err
	}
	if tableOutput == nil {
		d.log.Middleware.LogError(fmt.Sprintf("Unable to ftch table %s. Err: ", tableName), err)
	}

	schema := &models.TableKeySchema{
		TableName: tableName,
	}
	keySchemaElements := tableOutput.Table.KeySchema
	for _, keySchemaElement := range keySchemaElements {
		if keySchemaElement.KeyType == types.KeyTypeHash {
			schema.RangeKey = aws.ToString(keySchemaElement.AttributeName)
		} else if keySchemaElement.KeyType == types.KeyTypeRange {
			schema.RangeKey = aws.ToString(keySchemaElement.AttributeName)
		}
		if schema.PartitionKey != "" && schema.RangeKey != "" {
			break
		}
	}

	return schema, nil

}
