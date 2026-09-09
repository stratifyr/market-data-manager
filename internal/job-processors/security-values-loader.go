package jobprocessors

import (
	"fmt"
	"time"

	"github.com/stratifyr/security-service-proto/go/pb"
	"gofr.dev/pkg/gofr"

	client "github.com/stratifyr/security-service-client"

	dataProviders "github.com/stratifyr/market-data-manager/internal/data-providers"
)

type securityValuesLoader struct {
	dataProvider          dataProviders.Provider
	securityServiceClient client.SecurityServiceClient
}

func NewSecurityValuesLoader(dataProvider dataProviders.Provider, securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &securityValuesLoader{dataProvider: dataProvider, securityServiceClient: securityServiceClient}
}

func (l *securityValuesLoader) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(LoadSecurityValues)
	defer func() { recordJobCompletionLogs(logs, err) }()

	securities, err := l.securityServiceClient.GetSecurities(ctx, time.Now())
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

	ltpValues, err := l.dataProvider.EquityLTP(ctx, symbols)
	if err != nil {
		return logs, err
	}

	time.Sleep(1 * time.Second)

	ohlcvValues, err := l.dataProvider.EquityOHLCV(ctx, symbols)
	if err != nil {
		return logs, err
	}

	for i := range symbols {
		ltp, ok := ltpValues[symbols[i]]
		if !ok {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s ltp data not found", symbols[i]))
			continue
		}

		ohlcv, ok := ohlcvValues[symbols[i]]
		if !ok {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s volume data not found", symbols[i]))
			continue
		}

		payload := &pb.UpdateSecurityRequest{
			Id:     securityIDMap[symbols[i]],
			Ltp:    ltp,
			Volume: int64(ohlcv.Volume),
		}

		if err = l.securityServiceClient.UpdateSecurity(ctx, payload); err != nil {
			logs.Errors = append(logs.Errors, fmt.Sprint(symbols[i], err))
			continue
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s {ltp=%0.2f, vol=%d}", symbols[i], payload.Ltp, payload.Volume))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}
