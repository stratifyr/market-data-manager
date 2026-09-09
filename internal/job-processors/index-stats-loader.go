package jobprocessors

import (
	"fmt"
	"time"

	"gofr.dev/pkg/gofr"

	client "github.com/stratifyr/security-service-client"
	"github.com/stratifyr/security-service-proto/go/pb"

	dataProviders "github.com/stratifyr/market-data-manager/internal/data-providers"
)

type indexStatsLoader struct {
	dataProvider          dataProviders.Provider
	securityServiceClient client.SecurityServiceClient
}

func NewIndexStatsLoader(dataProvider dataProviders.Provider, securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &indexStatsLoader{dataProvider, securityServiceClient}
}

func (s *indexStatsLoader) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(LoadIndexStats)
	defer func() { recordJobCompletionLogs(logs, err) }()

	today := time.Now()

	marketDays, err := s.securityServiceClient.GetMarketDays(ctx, today, today)
	if err != nil {
		return logs, err
	}

	if len(marketDays) != 1 || marketDays[0].Format(time.DateOnly) != today.Format(time.DateOnly) {
		return logs, fmt.Errorf("cannot load stats on market holiday %v", today.Format(time.DateOnly))
	}

	indices, err := s.securityServiceClient.GetIndices(ctx, time.Now())
	if err != nil {
		return logs, err
	}

	var (
		indexNames = make([]string, len(indices))
		indexIDMap = make(map[string]int32)
	)

	for i := range indices {
		indexNames[i] = indices[i].Name
		indexIDMap[indices[i].Name] = indices[i].Id
	}

	ohlcData, err := s.dataProvider.IndexOHLC(ctx, indexNames)
	if err != nil {
		return logs, err
	}

	for _, indexName := range indexNames {
		ohlc, ok := ohlcData[indexName]
		if !ok {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s ohlc data not found", indexName))
			continue
		}

		payload := &pb.UpsertIndexStatRequest{
			IndexId: indexIDMap[indexName],
			Date:    today.Format(time.DateOnly),
			Open:    ohlc.Open,
			Close:   ohlc.Close,
			High:    ohlc.High,
			Low:     ohlc.Low,
		}

		if err = s.securityServiceClient.UpsertIndexStat(ctx, payload); err != nil {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s %v", indexName, err))
			continue
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s %s", indexName, ohlc))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}
