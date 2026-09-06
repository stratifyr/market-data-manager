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

type statsBackfiller struct {
	dataProvider          dataProviders.Provider
	securityServiceClient client.SecurityServiceClient
}

func NewStatsBackfiller(dataProvider dataProviders.Provider, securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &statsBackfiller{dataProvider: dataProvider, securityServiceClient: securityServiceClient}
}

func (s *statsBackfiller) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(BackfillSecurityStats)
	defer func() { recordJobCompletionLogs(logs, err) }()

	today := time.Now()
	fourYearsEarlier := today.AddDate(-4, 0, 0)
	startDate, endDate := fourYearsEarlier, today.AddDate(0, 0, -1)
	logs.Meta["backfill_period"] = fmt.Sprintf("%s - %s", startDate.Format(time.DateOnly), endDate.Format(time.DateOnly))

	securities, err := s.securityServiceClient.GetSecurities(ctx, time.Now())
	if err != nil {
		return logs, err
	}

	var (
		symbols       = make([]string, len(securities))
		securityIDMap = make(map[string]int32)
	)

	for i := range securities {
		symbols[i] = securities[i].Symbol
		securityIDMap[securities[i].Symbol] = securities[i].Id
	}

	marketDays, err := s.securityServiceClient.GetMarketDays(ctx, startDate, endDate)
	if err != nil {
		return logs, err
	}

	for i := range symbols {
		historicalData, err := s.dataProvider.HistoricalOHLC(ctx, symbols[i], startDate, endDate)
		if err != nil {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s %v", symbols[i], err))
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
				logs.Success = append(logs.Success, fmt.Sprintf("%s %s - %s",
					symbols[i], marketDays[0].Format(time.DateOnly), date.Format(time.DateOnly)))

				ctx.Logger.Info(logs.Success[len(logs.Success)-1])
			}

			idx := slices.IndexFunc(historicalData, func(ohlc *dataProviders.HistoricalOHLC) bool {
				return ohlc.Date.Format(time.DateOnly) == date.Format(time.DateOnly)
			})

			if idx == -1 {
				if j != len(marketDays)-1 {
					logs.Success = append(logs.Success, fmt.Sprintf("%s %s - %s",
						symbols[i], marketDays[0].Format(time.DateOnly), date.Format(time.DateOnly)))
				}

				ctx.Logger.Info(logs.Success[len(logs.Success)-1])

				break
			}

			payload := &pb.CreateOrUpdateSecurityStatRequest{
				SecurityId: securityIDMap[symbols[i]],
				Date:       date.Format(time.DateOnly),
				Open:       historicalData[idx].Open,
				Close:      historicalData[idx].Close,
				High:       historicalData[idx].High,
				Low:        historicalData[idx].Low,
				Volume:     int32(historicalData[idx].Volume),
			}

			if err = s.securityServiceClient.CreateOrUpdateSecurityStat(ctx, payload); err != nil {
				logs.Errors = append(logs.Errors, fmt.Sprintf("%s %s %v", symbols[i], date.Format(time.DateOnly), err))
				continue
			}
		}

	}

	return logs, nil
}
