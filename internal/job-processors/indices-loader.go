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
	"github.com/stratifyr/security-service-proto/go/pb"
	"gofr.dev/pkg/gofr"
	"gofr.dev/pkg/gofr/service"
)

type indicesLoader struct {
	securityServiceClient client.SecurityServiceClient
	indices               []string
}

func NewIndicesLoader(securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &indicesLoader{
		securityServiceClient: securityServiceClient,
		indices: []string{
			"NIFTY 50",
			"NIFTY NEXT 50",
			"NIFTY 100",
			"NIFTY MIDCAP 150",
			"NIFTY LARGEMIDCAP 250",
			"NIFTY SMALL CAP 250",
			"NIFTY MIDSMALLCAP 400",
			"NIFTY 500",
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

	for _, indexName := range l.indices {
		symbols, err = l.getIndexConstituents(ctx, indexName)
		if err != nil {
			return logs, err
		}

		var securityIDs []int32

		for _, symbol := range symbols {
			securityID, ok := securityIDBySymbol[symbol]
			if !ok {
				logs.Errors = append(logs.Errors, fmt.Sprintf("%s %s security id not found", indexName, symbol))
				continue
			}

			securityIDs = append(securityIDs, securityID)
		}

		payload := &pb.UpsertIndexRequest{
			Name:        indexName,
			SecurityIds: securityIDs,
		}

		if err = l.securityServiceClient.UpsertIndex(ctx, payload); err != nil {
			return logs, err
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s [%d]{%s}", indexName, len(symbols), strings.Join(symbols, ",")))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}

func (l *indicesLoader) getIndexConstituents(ctx *gofr.Context, indexName string) ([]string, error) {
	httpService := service.NewHTTPService("https://www.niftyindices.com", ctx.Logger, nil)
	fileName := fmt.Sprintf("ind_%slist.csv", strings.ReplaceAll(strings.ToLower(indexName), " ", ""))
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
