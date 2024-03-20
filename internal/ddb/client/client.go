package client

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/middleware"
)

func NewDynamoDBClient(ctx context.Context, endPointUrl, awsRegion string) (*dynamodb.Client, error) {
	middleware := middleware.NewMiddleware()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service string, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endPointUrl}, nil
			})),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("Sanjiv", "Madhavan", "S")),
	)
	if err != nil {
		middleware.LogFatal("Unable to create connection config", err)
		return nil, err
	}
	return dynamodb.NewFromConfig(cfg), nil
}
