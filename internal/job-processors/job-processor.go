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
	LoadLTP               = "LOAD_LTP"
	LoadSecurityStats     = "LOAD_SECURITY_STATS"
	BackfillSecurityStats = "BACKFILL_SECURITY_STATS"
	LoadIndices           = "LOAD_INDICES"
	LoadVolume            = "LOAD_VOLUME"
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
	case LoadLTP:
		return NewLtpLoader(dataProvider, securityServiceClient), nil
	case LoadSecurityStats:
		return NewStatsLoader(dataProvider, securityServiceClient), nil
	case BackfillSecurityStats:
		return NewStatsBackfiller(dataProvider, securityServiceClient), nil
	case LoadIndices:
		return NewIndicesLoader(securityServiceClient), nil
	case LoadVolume:
		return NewVolumeLoader(dataProvider, securityServiceClient), nil
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
