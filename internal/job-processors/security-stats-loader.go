package jobprocessors

import (
	"fmt"
	"time"

	"gofr.dev/pkg/gofr"

	client "github.com/stratifyr/security-service-client"
	"github.com/stratifyr/security-service-proto/go/pb"

	dataProviders "github.com/stratifyr/market-data-manager/internal/data-providers"
)

type securityStatsLoader struct {
	dataProvider          dataProviders.Provider
	securityServiceClient client.SecurityServiceClient
}

func NewSecurityStatsLoader(dataProvider dataProviders.Provider, securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &securityStatsLoader{dataProvider, securityServiceClient}
}

func (s *securityStatsLoader) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(LoadSecurityStats)
	defer func() { recordJobCompletionLogs(logs, err) }()

	today := time.Now()

	marketDays, err := s.securityServiceClient.GetMarketDays(ctx, today, today)
	if err != nil {
		return logs, err
	}

	if len(marketDays) != 1 || marketDays[0].Format(time.DateOnly) != today.Format(time.DateOnly) {
		return logs, fmt.Errorf("cannot load stats on market holiday %v", today.Format(time.DateOnly))
	}

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

	ohlcvData, err := s.dataProvider.EquityOHLCV(ctx, symbols)
	if err != nil {
		return logs, err
	}

	for i := range symbols {
		ohlcv, ok := ohlcvData[symbols[i]]
		if !ok {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s ohlcv data not found", symbols[i]))
			continue
		}

		payload := &pb.CreateOrUpdateSecurityStatRequest{
			SecurityId: securityIDMap[symbols[i]],
			Date:       today.Format(time.DateOnly),
			Open:       ohlcv.Open,
			Close:      ohlcv.Close,
			High:       ohlcv.High,
			Low:        ohlcv.Low,
			Volume:     int32(ohlcv.Volume),
		}

		if err = s.securityServiceClient.CreateOrUpdateSecurityStat(ctx, payload); err != nil {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s %v", symbols[i], err))
			continue
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s %s", symbols[i], ohlcv))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}
