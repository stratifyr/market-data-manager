package jobprocessors

import (
	"fmt"
	"time"

	"gofr.dev/pkg/gofr"

	client "github.com/stratifyr/security-service-client"

	dataProviders "github.com/stratifyr/market-data-manager/internal/data-providers"
)

type ltpLoader struct {
	dataProvider          dataProviders.Provider
	securityServiceClient client.SecurityServiceClient
}

func NewLtpLoader(dataProvider dataProviders.Provider, securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &ltpLoader{dataProvider: dataProvider, securityServiceClient: securityServiceClient}
}

func (l *ltpLoader) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(LoadLTP)
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

	ltpMap, err := l.dataProvider.LTP(ctx, symbols)
	if err != nil {
		return logs, fmt.Errorf("failed to get ltp data, err: %v", err)
	}

	for i := range symbols {
		ltp, ok := ltpMap[symbols[i]]
		if !ok {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s ltp data not found", symbols[i]))
			continue
		}

		if err = l.securityServiceClient.UpdateSecurityLTP(ctx, securityIDMap[symbols[i]], ltp); err != nil {
			logs.Errors = append(logs.Errors, fmt.Sprint(symbols[i], err))
			continue
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s %0.2f", symbols[i], ltp))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}
