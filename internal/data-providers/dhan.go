package dataproviders

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"gofr.dev/pkg/gofr"
	"gofr.dev/pkg/gofr/logging"
	"gofr.dev/pkg/gofr/service"
)

type client struct {
	clientID          string
	pin               string
	totpSecret        string
	dhanIDBySymbol    map[string]int
	dhanIDByIndexName map[string]int
	accessToken       atomic.Value
}

func NewDhanHQClient(app *gofr.App) (*client, error) {
	app.AddHTTPService("dhan-api", "https://api.dhan.co")
	app.AddHTTPService("dhan-auth-api", "https://auth.dhan.co")

	clientID := app.Config.Get("DHAN_CLIENT_ID")
	if clientID == "" {
		return nil, errors.New("missing DHAN_CLIENT_ID")
	}

	pin := app.Config.Get("DHAN_PIN")
	if pin == "" {
		return nil, errors.New("missing DHAN_PIN")
	}

	totpSecret := app.Config.Get("DHAN_TOTP_SECRET")
	if totpSecret == "" {
		return nil, errors.New("missing DHAN_TOTP_SECRET")
	}

	dhanIDBySymbol, err := extractDhanIDMappings(app.Logger(), "NSE_EQ", "EQ", "BE")
	if err != nil {
		return nil, err
	}

	dhanIDByIndexName, err := extractDhanIDMappings(app.Logger(), "IDX_I")
	if err != nil {
		return nil, err
	}

	return &client{
		clientID:          clientID,
		pin:               pin,
		totpSecret:        totpSecret,
		dhanIDBySymbol:    dhanIDBySymbol,
		dhanIDByIndexName: dhanIDByIndexName,
	}, nil
}

func (c *client) LTP(ctx *gofr.Context, symbols []string) (map[string]float64, error) {
	if len(symbols) > 1000 {
		return nil, errors.New("max limit is 1000 for bulk ltp fetch")
	}

	accessToken, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	payload := map[string][]int{
		"NSE_EQ": make([]int, len(symbols)),
	}

	for i := range symbols {
		payload["NSE_EQ"][i] = c.dhanIDBySymbol[symbols[i]]
	}

	body, _ := json.Marshal(payload)
	headers := map[string]string{"Content-Type": "application/json", "access-token": accessToken, "client-id": c.clientID}

	resp, err := ctx.GetHTTPService("dhan-api").PostWithHeaders(ctx, "v2/marketfeed/ltp", nil, body, headers)
	if err != nil {
		return nil, errors.New("failed POST /v2/marketfeed/ltp, err: " + err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)

		return nil, errors.New("non 200 resp POST /v2/marketfeed/ltp, resp: " + string(b))
	}

	var res struct {
		Data struct {
			NseEQ map[string]struct {
				LTP float64 `json:"last_price"`
			} `json:"NSE_EQ"`
		} `json:"data"`
	}

	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, errors.New("unexpected resp POST /v2/marketfeed/ltp, err: " + err.Error())
	}

	var ltpData = make(map[string]float64)

	for i := range symbols {
		securityID := c.dhanIDBySymbol[symbols[i]]

		data, ok := res.Data.NseEQ[strconv.Itoa(securityID)]
		if !ok {
			ctx.Warnf(fmt.Sprintf("missing data for %s, POST /v2/marketfeed/ltp", symbols[i]))
			continue
		}

		ltpData[symbols[i]] = data.LTP
	}

	return ltpData, nil
}

func (c *client) Volume(ctx *gofr.Context, symbols []string) (map[string]int, error) {
	ohlcvData, err := c.OHLC(ctx, symbols)
	if err != nil {
		return nil, err
	}

	var volumeData = make(map[string]int)

	for symbol, ohlcv := range ohlcvData {
		volumeData[symbol] = ohlcv.Volume
	}

	return volumeData, nil
}

func (c *client) OHLC(ctx *gofr.Context, symbols []string) (map[string]*OHLCData, error) {
	if len(symbols) > 1000 {
		return nil, errors.New("max limit is 1000 for bulk ltp fetch")
	}

	accessToken, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	payload := map[string][]int{
		"NSE_EQ": make([]int, len(symbols)),
	}

	for i := range symbols {
		payload["NSE_EQ"][i] = c.dhanIDBySymbol[symbols[i]]
	}

	body, _ := json.Marshal(payload)
	headers := map[string]string{"Content-Type": "application/json", "access-token": accessToken, "client-id": c.clientID}

	resp, err := ctx.GetHTTPService("dhan-api").PostWithHeaders(ctx, "v2/marketfeed/quote", nil, body, headers)
	if err != nil {
		return nil, errors.New("failed POST /v2/marketfeed/quote, err: " + err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)

		return nil, errors.New("non 200 resp POST /v2/marketfeed/quote, resp: " + string(b))
	}

	var res struct {
		Data struct {
			NseEQ map[string]struct {
				Volume float64 `json:"volume"`
				Ohlc   struct {
					Open  float64 `json:"open"`
					High  float64 `json:"high"`
					Low   float64 `json:"low"`
					Close float64 `json:"close"`
				} `json:"ohlc"`
			} `json:"NSE_EQ"`
		} `json:"data"`
	}

	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, errors.New("unexpected resp POST /v2/marketfeed/quote, err: " + err.Error())
	}

	var ohlcData = make(map[string]*OHLCData)

	for i := range symbols {
		securityID := c.dhanIDBySymbol[symbols[i]]

		data, ok := res.Data.NseEQ[strconv.Itoa(securityID)]
		if !ok {
			ctx.Warnf(fmt.Sprintf("missing data for %s, POST /v2/marketfeed/quote", symbols[i]))
			continue
		}

		ohlcData[symbols[i]] = &OHLCData{
			Open:   data.Ohlc.Open,
			High:   data.Ohlc.High,
			Low:    data.Ohlc.Low,
			Close:  data.Ohlc.Close,
			Volume: int(data.Volume),
		}
	}

	return ohlcData, nil
}

func (c *client) HistoricalOHLC(ctx *gofr.Context, symbol string, startDate, endDate time.Time) ([]*HistoricalOHLC, error) {
	accessToken, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	payload := map[string]any{
		"securityId":      c.dhanIDBySymbol[symbol],
		"exchangeSegment": "NSE_EQ",
		"instrument":      "EQUITY",
		"expiryCode":      0,
		"oi":              false,
		"fromDate":        startDate.Format(time.DateOnly),
		"toDate":          endDate.AddDate(0, 0, 1).Format(time.DateOnly),
	}

	body, _ := json.Marshal(payload)
	headers := map[string]string{"Content-Type": "application/json", "access-token": accessToken}

	resp, err := ctx.GetHTTPService("dhan-api").PostWithHeaders(ctx, "v2/charts/historical", nil, body, headers)
	if err != nil {
		return nil, errors.New("failed POST /v2/charts/historical, err: " + err.Error())
	}

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)

		return nil, errors.New("non 200 resp POST /v2/charts/historical, resp: " + string(b))
	}

	defer resp.Body.Close()

	var res struct {
		Open      []float64 `json:"open"`
		High      []float64 `json:"high"`
		Low       []float64 `json:"low"`
		Close     []float64 `json:"close"`
		Volume    []float64 `json:"volume"`
		Timestamp []float64 `json:"timestamp"`
	}

	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, errors.New("unexpected resp POST /v2/charts/historical, err: " + err.Error())
	}

	var historicalData = make([]*HistoricalOHLC, len(res.Timestamp))

	istLocation, _ := time.LoadLocation("Asia/Kolkata")

	for i := range res.Timestamp {
		historicalData[i] = &HistoricalOHLC{
			Date: time.Unix(int64(res.Timestamp[i]), 0).In(istLocation),
			OHLCData: &OHLCData{
				Open:   res.Open[i],
				High:   res.High[i],
				Low:    res.Low[i],
				Close:  res.Close[i],
				Volume: int(res.Volume[i]),
			},
		}
	}

	return historicalData, nil
}

func (c *client) IndexValue(ctx *gofr.Context, indexNames []string) (map[string]float64, error) {
	if len(indexNames) > 1000 {
		return nil, errors.New("max limit is 1000 for /v2/marketfeed/ltp")
	}

	accessToken, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	payload := map[string][]int{
		"IDX_I": make([]int, len(indexNames)),
	}

	for i := range indexNames {
		payload["IDX_I"][i] = c.dhanIDByIndexName[getDhanIndexName(indexNames[i])]
	}

	body, _ := json.Marshal(payload)
	headers := map[string]string{"Content-Type": "application/json", "access-token": accessToken, "client-id": c.clientID}

	resp, err := ctx.GetHTTPService("dhan-api").PostWithHeaders(ctx, "v2/marketfeed/ltp", nil, body, headers)
	if err != nil {
		return nil, errors.New("failed POST /v2/marketfeed/ltp, err: " + err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)

		return nil, errors.New("non 200 resp POST /v2/marketfeed/ltp, resp: " + string(b))
	}

	var res struct {
		Data struct {
			IdxI map[string]struct {
				Value float64 `json:"last_price"`
			} `json:"IDX_I"`
		} `json:"data"`
	}

	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, errors.New("unexpected resp POST /v2/marketfeed/ltp, err: " + err.Error())
	}

	var valueData = make(map[string]float64)

	for i := range indexNames {
		securityID := c.dhanIDByIndexName[getDhanIndexName(indexNames[i])]

		data, ok := res.Data.IdxI[strconv.Itoa(securityID)]
		if !ok {
			ctx.Warnf(fmt.Sprintf("missing data for %s, POST /v2/marketfeed/ltp", indexNames[i]))
			continue
		}

		valueData[indexNames[i]] = data.Value
	}

	return valueData, nil
}

func (c *client) IndexOHLC(ctx *gofr.Context, indexNames []string) (map[string]*OHLCData, error) {
	if len(indexNames) > 1000 {
		return nil, errors.New("max limit is 1000 for /v2/marketfeed/quote")
	}

	accessToken, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	payload := map[string][]int{
		"IDX_I": make([]int, len(indexNames)),
	}

	for i := range indexNames {
		payload["IDX_I"][i] = c.dhanIDByIndexName[getDhanIndexName(indexNames[i])]
	}

	body, _ := json.Marshal(payload)
	headers := map[string]string{"Content-Type": "application/json", "access-token": accessToken, "client-id": c.clientID}

	resp, err := ctx.GetHTTPService("dhan-api").PostWithHeaders(ctx, "v2/marketfeed/quote", nil, body, headers)
	if err != nil {
		return nil, errors.New("failed POST /v2/marketfeed/quote, err: " + err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)

		return nil, errors.New("non 200 resp POST /v2/marketfeed/quote, resp: " + string(b))
	}

	var res struct {
		Data struct {
			IdxI map[string]struct {
				Volume float64 `json:"volume"`
				Ohlc   struct {
					Open  float64 `json:"open"`
					High  float64 `json:"high"`
					Low   float64 `json:"low"`
					Close float64 `json:"close"`
				} `json:"ohlc"`
			} `json:"IDX_I"`
		} `json:"data"`
	}

	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, errors.New("unexpected resp POST /v2/marketfeed/quote, err: " + err.Error())
	}

	var ohlcData = make(map[string]*OHLCData)

	for i := range indexNames {
		securityID := c.dhanIDByIndexName[getDhanIndexName(indexNames[i])]

		data, ok := res.Data.IdxI[strconv.Itoa(securityID)]
		if !ok {
			ctx.Warnf(fmt.Sprintf("missing data for %s, POST /v2/marketfeed/quote", indexNames[i]))
			continue
		}

		ohlcData[indexNames[i]] = &OHLCData{
			Open:   data.Ohlc.Open,
			High:   data.Ohlc.High,
			Low:    data.Ohlc.Low,
			Close:  data.Ohlc.Close,
			Volume: int(data.Volume),
		}
	}

	return ohlcData, nil
}

func (c *client) IndexHistoricalOHLC(ctx *gofr.Context, indexName string, startDate, endDate time.Time) ([]*HistoricalOHLC, error) {
	accessToken, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	payload := map[string]any{
		"securityId":      c.dhanIDByIndexName[getDhanIndexName(indexName)],
		"exchangeSegment": "IDX_I",
		"instrument":      "INDEX",
		"expiryCode":      0,
		"oi":              false,
		"fromDate":        startDate.Format(time.DateOnly),
		"toDate":          endDate.AddDate(0, 0, 1).Format(time.DateOnly),
	}

	body, _ := json.Marshal(payload)
	headers := map[string]string{"Content-Type": "application/json", "access-token": accessToken}

	resp, err := ctx.GetHTTPService("dhan-api").PostWithHeaders(ctx, "v2/charts/historical", nil, body, headers)
	if err != nil {
		return nil, errors.New("failed POST /v2/charts/historical, err: " + err.Error())
	}

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)

		return nil, errors.New("non 200 resp POST /v2/charts/historical, resp: " + string(b))
	}

	defer resp.Body.Close()

	var res struct {
		Open      []float64 `json:"open"`
		High      []float64 `json:"high"`
		Low       []float64 `json:"low"`
		Close     []float64 `json:"close"`
		Volume    []float64 `json:"volume"`
		Timestamp []float64 `json:"timestamp"`
	}

	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, errors.New("unexpected resp POST /v2/charts/historical, err: " + err.Error())
	}

	var historicalData = make([]*HistoricalOHLC, len(res.Timestamp))

	istLocation, _ := time.LoadLocation("Asia/Kolkata")

	for i := range res.Timestamp {
		historicalData[i] = &HistoricalOHLC{
			Date: time.Unix(int64(res.Timestamp[i]), 0).In(istLocation),
			OHLCData: &OHLCData{
				Open:   res.Open[i],
				High:   res.High[i],
				Low:    res.Low[i],
				Close:  res.Close[i],
				Volume: int(res.Volume[i]),
			},
		}
	}

	return historicalData, nil
}

func (c *client) getAccessToken(ctx *gofr.Context) (string, error) {
	return "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyUmVnaW9uIjoiUjEiLCJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzg4OTYxNDU5LCJpYXQiOjE3ODg4NzUwNTksInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA2ODU0MDQ4In0.6SiFc5CnTDP81te2DOHnhCXALHlgkooERMJ-0FxpU462aGKY65Nt_YSqjmYxE20S7wbmLLbjFExZeAaaTmbmkw", nil

	token, ok := c.accessToken.Load().(string)
	if ok && !c.isTokenExpired(token) {
		return token, nil
	}

	newToken, err := c.generateAccessToken(ctx)
	if err != nil {
		return "", err
	}

	c.accessToken.Store(newToken)

	return newToken, nil
}

func (c *client) isTokenExpired(tokenString string) bool {
	var claims jwt.MapClaims

	_, _, err := jwt.NewParser().ParseUnverified(tokenString, &claims)
	if err != nil {
		return true
	}

	exp, err := claims.GetExpirationTime()
	if err != nil {
		return true
	}

	return exp.Before(time.Now().UTC())
}

func (c *client) generateAccessToken(ctx *gofr.Context) (string, error) {
	params := map[string]any{"dhanClientId": c.clientID, "pin": c.pin, "totp": c.generateTOTP()}

	resp, err := ctx.GetHTTPService("dhan-auth-api").Post(ctx, "app/generateAccessToken", params, nil)
	if err != nil {
		return "", errors.New("failed POST /v2/app/generateAccessToken, err: " + err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)

		return "", fmt.Errorf("non 200 resp POST /app/generateAccessToken, resp: %s", string(b))
	}

	var res struct {
		Status      string `json:"status"`
		Message     string `json:"message"`
		AccessToken string `json:"accessToken"`
	}

	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", fmt.Errorf("unexpected resp POST /app/generateAccessToken: %w", err)
	}

	if res.Status == "error" {
		return "", fmt.Errorf("failed to generate access token, msg: %s", res.Message)
	}

	return res.AccessToken, nil
}

func (c *client) generateTOTP() string {
	now := time.Now()

	// If less than 5 seconds remain in the current TOTP window, wait for the next window.
	remaining := 30 - (now.Unix() % 30)

	if remaining <= 5 {
		time.Sleep(time.Duration(remaining+1) * time.Second)
		now = time.Now()
	}

	key, _ := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(c.totpSecret)
	counter := uint64(now.Unix() / 30)

	var counterBytes [8]byte
	binary.BigEndian.PutUint64(counterBytes[:], counter)

	mac := hmac.New(sha1.New, key)
	mac.Write(counterBytes[:])
	hash := mac.Sum(nil)

	offset := hash[len(hash)-1] & 0x0f

	code := (uint32(hash[offset])&0x7f)<<24 |
		(uint32(hash[offset+1])&0xff)<<16 |
		(uint32(hash[offset+2])&0xff)<<8 |
		(uint32(hash[offset+3]) & 0xff)

	return fmt.Sprintf("%06d", code%1000000)
}

func getDhanIndexName(indexName string) string {
	switch indexName {
	case "NIFTY 50":
		return "NIFTY"
	case "NIFTY NEXT 50":
		return "NIFTYNXT50"
	case "NIFTY LARGEMIDCAP 250":
		return "NIFTYLARGEMID250"
	default:
		return strings.ReplaceAll(indexName, " ", "")
	}
}

func extractDhanIDMappings(logger logging.Logger, segment string, series ...string) (map[string]int, error) {
	httpService := service.NewHTTPService("https://api.dhan.co", logger, nil)
	apiName := fmt.Sprintf("v2/instrument/%s", segment)

	resp, err := httpService.Get(context.TODO(), apiName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed GET /%s, err: %v", apiName, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return nil, fmt.Errorf("non 200 resp GET /%s, err: %s", apiName, body)
	}

	reader := csv.NewReader(resp.Body)
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read dhan csv headers: %w", err)
	}

	idx := func(col string) (int, error) {
		i := slices.Index(headers, col)
		if i < 0 {
			return -1, fmt.Errorf("missing column %q in dhan csv", col)
		}

		return i, nil
	}

	idxUnderlyingSymbol, err := idx("UNDERLYING_SYMBOL")
	if err != nil {
		return nil, err
	}

	idxSecurityID, err := idx("SECURITY_ID")
	if err != nil {
		return nil, err
	}

	idxSeries, err := idx("SERIES")
	if err != nil {
		return nil, err
	}

	underlyingSymbolToDhanID := make(map[string]int)

	rowNo := 1
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("read dhan csv row %d: %w", rowNo, err)
		}

		rowNo++

		if len(series) > 0 && !slices.Contains(series, row[idxSeries]) {
			continue
		}

		underlyingSymbol := strings.ReplaceAll(row[idxUnderlyingSymbol], " ", "")
		id, _ := strconv.Atoi(row[idxSecurityID])

		underlyingSymbolToDhanID[underlyingSymbol] = id
	}

	return underlyingSymbolToDhanID, nil
}
