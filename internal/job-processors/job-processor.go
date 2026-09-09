package jobprocessors

import (
	"fmt"
	"strings"
	"time"

	"gofr.dev/pkg/gofr"

	client "github.com/stratifyr/security-service-client"

	dataProviders "github.com/stratifyr/market-data-manager/internal/data-providers"
)

const (
	LoadSecurities        = "LOAD_SECURITIES"
	LoadSecurityValues    = "LOAD_SECURITY_VALUES"
	LoadSecurityStats     = "LOAD_SECURITY_STATS"
	LoadSecurityShares    = "LOAD_SECURITY_SHARES"
	BackfillSecurityStats = "BACKFILL_SECURITY_STATS"
	LoadIndices           = "LOAD_INDICES"
	LoadIndexValues       = "LOAD_INDEX_VALUES"
	LoadIndexStats        = "LOAD_INDEX_STATS"
	BackfillIndexStats    = "BACKFILL_INDEX_STATS"
)

type JobProcessor interface {
	Process(ctx *gofr.Context) (logs *Logs, err error)
}

type Logs struct {
	Job     string            `json:"job"`
	Meta    map[string]string `json:"meta"`
	Success []string          `json:"success"`
	Errors  []string          `json:"errors"`
}

func GetJobProcessor(marketDataJob string, dataProvider dataProviders.Provider, securityServiceClient client.SecurityServiceClient) (JobProcessor, error) {
	switch marketDataJob {
	case LoadSecurityValues:
		return NewSecurityValuesLoader(dataProvider, securityServiceClient), nil
	case LoadSecurityStats:
		return NewSecurityStatsLoader(dataProvider, securityServiceClient), nil
	case LoadSecurityShares:
		return NewSecuritySharesLoader(securityServiceClient), nil
	case BackfillSecurityStats:
		return NewSecurityStatsBackfiller(dataProvider, securityServiceClient), nil
	case LoadIndices:
		return NewIndicesLoader(securityServiceClient), nil
	case LoadIndexValues:
		return NewIndexValuesLoader(dataProvider, securityServiceClient), nil
	case LoadIndexStats:
		return NewIndexStatsLoader(dataProvider, securityServiceClient), nil
	case BackfillIndexStats:
		return NewIndexStatsBackfiller(dataProvider, securityServiceClient), nil
	default:
		return nil, fmt.Errorf("invalid market data job type: %s", marketDataJob)
	}
}

func initializeJobLogs(jobName string) *Logs {
	return &Logs{
		Job: strings.ToLower(jobName),
		Meta: map[string]string{
			"start_time": time.Now().Format(time.DateTime),
		},
	}
}

func recordJobCompletionLogs(logs *Logs, err error) {
	if err != nil {
		logs.Errors = append(logs.Errors, err.Error())
	}

	logs.Meta["end_time"] = time.Now().Format(time.DateTime)
}
