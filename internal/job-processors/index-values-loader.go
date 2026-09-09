package jobprocessors

import (
	"fmt"
	"time"

	"github.com/stratifyr/security-service-proto/go/pb"
	"gofr.dev/pkg/gofr"

	client "github.com/stratifyr/security-service-client"

	dataProviders "github.com/stratifyr/market-data-manager/internal/data-providers"
)

type indexValuesLoader struct {
	dataProvider          dataProviders.Provider
	securityServiceClient client.SecurityServiceClient
}

func NewIndexValuesLoader(dataProvider dataProviders.Provider, securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &indexValuesLoader{dataProvider: dataProvider, securityServiceClient: securityServiceClient}
}

func (l *indexValuesLoader) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(LoadIndexValues)
	defer func() { recordJobCompletionLogs(logs, err) }()

	indices, err := l.securityServiceClient.GetIndices(ctx, time.Now())
	if err != nil {
		return logs, err
	}

	var (
		indexNames = make([]string, len(indices))
	)

	for i := range indices {
		indexNames[i] = indices[i].Name
	}

	valuesMap, err := l.dataProvider.IndexValue(ctx, indexNames)
	if err != nil {
		return logs, err
	}

	for _, indexName := range indexNames {
		value, ok := valuesMap[indexName]
		if !ok {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s value not found", indexName))
			continue
		}

		payload := &pb.UpsertIndexRequest{Name: indexName, Value: value}

		if err = l.securityServiceClient.UpsertIndex(ctx, payload); err != nil {
			logs.Errors = append(logs.Errors, fmt.Sprint(indexName, err))
			continue
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s {val=%0.2f}", indexName, value))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}
