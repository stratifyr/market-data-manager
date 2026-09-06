package jobprocessors

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	client "github.com/stratifyr/security-service-client"
	"gofr.dev/pkg/gofr"
	"gofr.dev/pkg/gofr/service"
)

type indicesLoader struct {
	securityServiceClient client.SecurityServiceClient
	indices               map[string]string
}

func NewIndicesLoader(securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &indicesLoader{
		securityServiceClient: securityServiceClient,
		indices: map[string]string{
			"NIFTY 50":              "ind_nifty50list.csv",
			"NIFTY NEXT 50":         "ind_niftynext50list.csv",
			"NIFTY 100":             "ind_nifty100list.csv",
			"NIFTY MIDCAP 150":      "ind_niftymidcap150list.csv",
			"NIFTY LARGEMIDCAP 250": "ind_niftylargemidcap250list.csv",
			"NIFTY SMALL CAP 250":   "ind_niftysmallcap250list.csv",
			"NIFTY MIDSMALLCAP 400": "ind_niftymidsmallcap400list.csv",
			"NIFTY 500":             "ind_nifty500list.csv",
		},
	}
}

func (l *indicesLoader) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(LoadIndices)
	defer func() { recordJobCompletionLogs(logs, err) }()

	securities, err := l.securityServiceClient.GetSecurities(ctx, time.Now())
	if err != nil {
		return logs, err
	}

	var securityIDBySymbol = make(map[string]int32)

	for i := range securities {
		securityIDBySymbol[securities[i].Symbol] = securities[i].Id
	}

	var symbols []string

	for indexName, constituentsFile := range l.indices {
		symbols, err = l.getIndexConstituents(ctx, constituentsFile)
		if err != nil {
			return logs, err
		}

		var securityIDs []int32

		for _, symbol := range symbols {
			securityID, ok := securityIDBySymbol[symbol]
			if !ok {
				logs.Errors = append(logs.Errors, fmt.Sprintf("%s security id not found", symbol))
				continue
			}

			securityIDs = append(securityIDs, securityID)
		}

		if err = l.securityServiceClient.UpsertIndex(ctx, indexName, securityIDs); err != nil {
			return logs, err
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s [%d]{%s}", indexName, len(symbols), strings.Join(symbols, ",")))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}

func (l *indicesLoader) getIndexConstituents(ctx *gofr.Context, fileName string) ([]string, error) {
	httpService := service.NewHTTPService("https://www.niftyindices.com", ctx.Logger, nil)
	apiName := fmt.Sprintf("IndexConstituent/%s", fileName)

	resp, err := httpService.GetWithHeaders(ctx, apiName, nil, map[string]string{"User-Agent": "Mozilla/5.0"})
	if err != nil {
		return nil, fmt.Errorf("failed GET /%s, err: %v", apiName, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("non 200 resp GET /%s, resp: %s", apiName, string(body))
	}

	reader := csv.NewReader(resp.Body)
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read %s headers, err: %s", fileName, err)
	}

	idx := func(col string) (int, error) {
		i := slices.Index(headers, col)
		if i < 0 {
			return -1, fmt.Errorf("missing column %q in %s", col, fileName)
		}

		return i, nil
	}

	idxSymbol, err := idx("Symbol")
	if err != nil {
		return nil, err
	}

	var (
		symbols []string
		rowNo   = 1
	)

	for {
		row, rowErr := reader.Read()
		if rowErr == io.EOF {
			break
		}

		if rowErr != nil {
			return nil, fmt.Errorf("failed to read %s row %d, err: %v", fileName, rowNo, rowErr)
		}

		rowNo++

		symbol := strings.TrimSpace(row[idxSymbol])

		symbols = append(symbols, symbol)
	}

	return symbols, nil
}
