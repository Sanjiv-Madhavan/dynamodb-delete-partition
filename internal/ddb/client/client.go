package client

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/constants"
)

func NewDynamoDBClient(ctx context.Context, endPointUrl, awsRegion string) (*dynamodb.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service string, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endPointUrl}, nil
			})),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("Sanjiv", "Madhavan", "S")),
	)
	if err != nil {
		slog.Error("Error while getting connection",
			constants.CliRequestId, ctx.Value(constants.CliRequestId),
			constants.LogErrorKey, err)
		return nil, err
	}
	return dynamodb.NewFromConfig(cfg), nil
}
