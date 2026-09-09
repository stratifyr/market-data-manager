package jobprocessors

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	client "github.com/stratifyr/security-service-client"
	"github.com/stratifyr/security-service-proto/go/pb"
	"gofr.dev/pkg/gofr"
	"gofr.dev/pkg/gofr/service"
)

type securitySharesLoader struct {
	securityServiceClient client.SecurityServiceClient
}

func NewSecuritySharesLoader(securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &securitySharesLoader{securityServiceClient: securityServiceClient}
}

func (l *securitySharesLoader) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(LoadSecurityShares)
	defer func() { recordJobCompletionLogs(logs, err) }()

	securities, err := l.securityServiceClient.GetSecurities(ctx, time.Now())
	if err != nil {
		return logs, err
	}

	var (
		symbols            = make([]string, len(securities))
		securityIDBySymbol = make(map[string]int32)
	)

	for i := range securities {
		symbols[i] = securities[i].Symbol
		securityIDBySymbol[securities[i].Symbol] = securities[i].Id
	}

	freeFloatSharesMap, err := l.getFreeFloatShares(ctx, symbols)
	if err != nil {
		return logs, err
	}

	for i := range symbols {
		freeFloatShares, ok := freeFloatSharesMap[symbols[i]]
		if !ok {
			logs.Errors = append(logs.Errors, fmt.Sprintf("%s pre open data not found", symbols[i]))
			continue
		}

		payload := &pb.UpdateSecurityRequest{
			Id:              securityIDBySymbol[symbols[i]],
			FreeFloatShares: int64(freeFloatShares),
		}

		if err = l.securityServiceClient.UpdateSecurity(ctx, payload); err != nil {
			logs.Errors = append(logs.Errors, fmt.Sprint(symbols[i], err))
			continue
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s {shares=%d}", symbols[i], freeFloatShares))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}

func (l *securitySharesLoader) getFreeFloatShares(ctx *gofr.Context, symbols []string) (map[string]int, error) {
	var targetSymbols = make(map[string]struct{})
	for _, symbol := range symbols {
		targetSymbols[symbol] = struct{}{}
	}

	httpService := service.NewHTTPService("https://www.nseindia.com", ctx.Logger, nil)
	apiName := "api/NextApi/apiClient/cmPreOpenApi"
	queryParams := map[string]any{"functionName": "getPreOpenData", "category": "ALL", "symbol": ""}

	resp, err := httpService.GetWithHeaders(ctx, apiName, queryParams, map[string]string{"User-Agent": "Mozilla/5.0"})
	if err != nil {
		return nil, fmt.Errorf("failed GET /%s, err: %v", apiName, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("non 200 resp GET /%s, resp: %s", apiName, string(body))
	}

	var res struct {
		Data []*struct {
			Symbol      string  `json:"symbol"`
			PrevClose   float64 `json:"prevClose"`
			FFMarketCap float64 `json:"ffmMarketCap"`
		} `json:"data"`
	}

	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, fmt.Errorf("unexpected resp GET /%s, err: %v", apiName, err)
	}

	var freeFloatShares = make(map[string]int)

	for i := range res.Data {
		symbol := res.Data[i].Symbol
		if _, ok := targetSymbols[symbol]; ok {
			freeFloatShares[symbol] = int(res.Data[i].FFMarketCap / res.Data[i].PrevClose)
		}
	}

	return freeFloatShares, nil
}
