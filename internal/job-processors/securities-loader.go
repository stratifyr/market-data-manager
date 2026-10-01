package jobprocessors

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	client "github.com/stratifyr/security-service-client"
	"github.com/stratifyr/security-service-proto/go/pb"
	"gofr.dev/pkg/gofr"
	"gofr.dev/pkg/gofr/service"
)

type securitiesLoader struct {
	securityServiceClient client.SecurityServiceClient
}

func NewSecuritiesLoader(securityServiceClient client.SecurityServiceClient) JobProcessor {
	return &securitiesLoader{
		securityServiceClient: securityServiceClient,
	}
}

func (l *securitiesLoader) Process(ctx *gofr.Context) (logs *Logs, err error) {
	logs = initializeJobLogs(LoadSecurities)
	defer func() { recordJobCompletionLogs(logs, err) }()

	securities, err := l.getNifty500Securities(ctx)
	if err != nil {
		return logs, err
	}

	for _, payload := range securities {
		if err = l.securityServiceClient.UpsertSecurity(ctx, payload); err != nil {
			return logs, err
		}

		logs.Success = append(logs.Success, fmt.Sprintf("%s {isin=%s, industry=%s, name=%s}", payload.Symbol, payload.Isin, payload.Industry, payload.Name))
		ctx.Logger.Info(logs.Success[len(logs.Success)-1])
	}

	return logs, nil
}

func (l *securitiesLoader) getNifty500Securities(ctx *gofr.Context) ([]*pb.UpsertSecurityRequest, error) {
	httpService := service.NewHTTPService("https://www.niftyindices.com", ctx.Logger, nil)
	fileName := "ind_nifty500list.csv"
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

	idxISIN, err := idx("ISIN Code")
	if err != nil {
		return nil, err
	}

	idxSymbol, err := idx("Symbol")
	if err != nil {
		return nil, err
	}

	idxIndustry, err := idx("Industry")
	if err != nil {
		return nil, err
	}

	idxName, err := idx("Company Name")
	if err != nil {
		return nil, err
	}

	var (
		securities []*pb.UpsertSecurityRequest
		rowNo      = 1
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

		security := &pb.UpsertSecurityRequest{
			Isin:     strings.TrimSpace(row[idxISIN]),
			Symbol:   strings.TrimSpace(row[idxSymbol]),
			Industry: strings.TrimSpace(row[idxIndustry]),
			Name:     strings.TrimSpace(row[idxName]),
		}

		if strings.Contains(security.Symbol, "DUMMY") {
			ctx.Logger.Warnf("skipping %s in %s", security.Symbol, fileName)
			continue
		}

		securities = append(securities, security)
	}

	return securities, nil
}
