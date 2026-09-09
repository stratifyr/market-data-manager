package jobprocessors

import (
	"fmt"
	"slices"
	"time"

	"gofr.dev/pkg/gofr"

	client "github.com/stratifyr/security-service-client"
	"github.com/stratifyr/security-service-proto/go/pb"

	dataProviders "github.com/stratifyr/market-data-manager/internal/data-providers"
)

type indexStatsBackfiller struct {
	dataProvider          dataProviders.Provider
	securityServiceClient client.SecurityServiceClient
}

func NewIndexStatsBackfiller(dataProvider dataProviders.Provider, securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &indexStatsBackfiller{dataProvider: dataProvider, securityServiceClient: securityServiceClient}
}

func (s *indexStatsBackfiller) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(BackfillIndexStats)
	defer func() { recordJobCompletionLogs(logs, err) }()

	today := time.Now()
	fourYearsEarlier := today.AddDate(-4, 0, 0)
	startDate, endDate := fourYearsEarlier, today.AddDate(0, 0, -1)
	logs.Meta["backfill_period"] = fmt.Sprintf("%s - %s", startDate.Format(time.DateOnly), endDate.Format(time.DateOnly))

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

	marketDays, err := s.securityServiceClient.GetMarketDays(ctx, startDate, endDate)
	if err != nil {
		return logs, err
	}

	for i := range indexNames {
		historicalData, err := s.dataProvider.IndexHistoricalOHLCV(ctx, indexNames[i], startDate, endDate)
		if err != nil {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s %v", indexNames[i], err))
			continue
		}

		slices.SortFunc(marketDays, func(a, b time.Time) int {
			if a.After(b) {
				return -1
			}

			return 1
		})

		for j, date := range marketDays {
			if j == len(marketDays)-1 {
				logs.Success = append(logs.Success, fmt.Sprintf("%s {start=%s end=%s}",
					indexNames[i], marketDays[0].Format(time.DateOnly), date.Format(time.DateOnly)))

				ctx.Logger.Info(logs.Success[len(logs.Success)-1])
			}

			idx := slices.IndexFunc(historicalData, func(ohlc *dataProviders.HistoricalOHLCV) bool {
				return ohlc.Date.Format(time.DateOnly) == date.Format(time.DateOnly)
			})

			if idx == -1 {
				if j != len(marketDays)-1 {
					logs.Success = append(logs.Success, fmt.Sprintf("%s {start=%s end=%s}",
						indexNames[i], marketDays[0].Format(time.DateOnly), date.Format(time.DateOnly)))
				}

				ctx.Logger.Info(logs.Success[len(logs.Success)-1])

				break
			}

			payload := &pb.UpsertIndexStatRequest{
				IndexId: indexIDMap[indexNames[i]],
				Date:    date.Format(time.DateOnly),
				Open:    historicalData[idx].Open,
				Close:   historicalData[idx].Close,
				High:    historicalData[idx].High,
				Low:     historicalData[idx].Low,
			}

			if err = s.securityServiceClient.UpsertIndexStat(ctx, payload); err != nil {
				logs.Errors = append(logs.Errors, fmt.Sprintf("%s %s %v", indexNames[i], date.Format(time.DateOnly), err))
				continue
			}
		}

	}

	return logs, nil
}
