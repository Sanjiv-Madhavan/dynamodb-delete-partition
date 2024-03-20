package interfaces

import (
	"context"

	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/models"
)

type TablePartitionHandler interface {
	HandleTablePartitionDeletion(ctx context.Context, dtpi models.DeleteTablePartitionInput) error
}
