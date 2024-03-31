package handler

import (
	"context"

	"github.com/sanjiv-madhavan/dynamodb-delete-partition/interfaces"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/domainmodel"
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/models"
)

type TablePartitionHandler struct {
	deletionService interfaces.TablePartitionDeleter
}

func NewTablePartitionHandler(ctx context.Context, endpointURL string, awsRegion string) (*TablePartitionHandler, error) {

	domainModelService, err := domainmodel.NewTablePartitionService(ctx, endpointURL, awsRegion)
	if err != nil {
		return nil, err
	}
	return &TablePartitionHandler{
		deletionService: domainModelService,
	}, nil
}

func (handler *TablePartitionHandler) HandleTablePartitionDeletion(ctx context.Context, dtpi models.DeleteTablePartitionInput) error {
	return handler.deletionService.DeletePartition(ctx, dtpi)
}
