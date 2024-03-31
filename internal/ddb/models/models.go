package models

type DeleteTablePartitionInput struct {
	TableName      string
	PartitionValue string
	EndpointUrl    string
	AwsRegion      string
}

type TableKeySchema struct {
	TableName    string
	PartitionKey string
	RangeKey     string
}

type RequestId string
