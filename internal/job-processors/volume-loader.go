package jobprocessors

import (
	"fmt"
	"time"

	"gofr.dev/pkg/gofr"

	client "github.com/stratifyr/security-service-client"

	dataProviders "github.com/stratifyr/market-data-manager/internal/data-providers"
)

type volumeLoader struct {
	dataProvider          dataProviders.Provider
	securityServiceClient client.SecurityServiceClient
}

func NewVolumeLoader(dataProvider dataProviders.Provider, securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &volumeLoader{dataProvider: dataProvider, securityServiceClient: securityServiceClient}
}

func (l *volumeLoader) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(LoadVolume)
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

	volumeMap, err := l.dataProvider.Volume(ctx, symbols)
	if err != nil {
		return logs, fmt.Errorf("failed to get volume data, err: %v", err)
	}

	for i := range symbols {
		volume, ok := volumeMap[symbols[i]]
		if !ok {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s quote data not found", symbols[i]))
			continue
		}

		if err = l.securityServiceClient.UpdateSecurityVolume(ctx, securityIDMap[symbols[i]], int64(volume)); err != nil {
			logs.Errors = append(logs.Errors, fmt.Sprint(symbols[i], err))
			continue
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s %0d", symbols[i], volume))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}
