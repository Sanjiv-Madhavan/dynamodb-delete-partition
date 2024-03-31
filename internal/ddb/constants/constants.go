package constants

import "github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/models"

const (
	CliRequestId    models.RequestId = "request-id"
	LogRequestIdKey                  = "request-id"
	LogErrorKey                      = "error"
)

const BatchWriteItemSize = 25
