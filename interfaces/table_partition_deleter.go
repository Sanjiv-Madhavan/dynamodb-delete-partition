package interfaces

import (
	"context"

	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/models"
)

type TablePartitionDeleter interface {
	DeleteTablePartition(ctx context.Context, dtpi models.DeleteTablePartitionInput) error
}
