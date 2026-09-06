package dataproviders

import (
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
)

type client struct {
	clientID       string
	pin            string
	totpSecret     string
	symbolToDhanID map[string]int
	dhanIDToSymbol map[int]string
	accessToken    atomic.Value
}

func NewDhanHQClient(app *gofr.App) (*client, error) {
	app.AddHTTPService("dhan-api", "https://api.dhan.co")
	app.AddHTTPService("dhan-auth-api", "https://auth.dhan.co")

	symbolToID, idToSymbol, err := extractDhanIDMappings()
	if err != nil {
		return nil, err
	}

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

	return &client{
		clientID:       clientID,
		pin:            pin,
		totpSecret:     totpSecret,
		symbolToDhanID: symbolToID,
		dhanIDToSymbol: idToSymbol,
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
		payload["NSE_EQ"][i] = c.symbolToDhanID[symbols[i]]
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
		securityID := c.symbolToDhanID[symbols[i]]

		data, ok := res.Data.NseEQ[strconv.Itoa(securityID)]
		if !ok {
			ctx.Warnf(fmt.Sprintf("missing data for %s, POST /v2/marketfeed/ltp", symbols[i]))
			continue
		}

		ltpData[symbols[i]] = data.LTP
	}

	return ltpData, nil
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
		payload["NSE_EQ"][i] = c.symbolToDhanID[symbols[i]]
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
		securityID := c.symbolToDhanID[symbols[i]]

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
		"securityId":      c.symbolToDhanID[symbol],
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

func (c *client) getAccessToken(ctx *gofr.Context) (string, error) {
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

func extractDhanIDMappings() (map[string]int, map[int]string, error) {
	req, err := http.NewRequest(http.MethodGet, "https://api.dhan.co/v2/instrument/NSE_EQ", nil)
	if err != nil {
		return nil, nil, err
	}

	c := &http.Client{Timeout: 30 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("download dhan scrip master: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("dhan scrip master http %s", resp.Status)
	}

	reader := csv.NewReader(resp.Body)
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("read dhan csv headers: %w", err)
	}

	idx := func(col string) (int, error) {
		i := slices.Index(headers, col)
		if i < 0 {
			return -1, fmt.Errorf("missing column %q in dhan csv", col)
		}

		return i, nil
	}

	idxSymbol, err := idx("UNDERLYING_SYMBOL")
	if err != nil {
		return nil, nil, err
	}

	idxSecurityID, err := idx("SECURITY_ID")
	if err != nil {
		return nil, nil, err
	}

	idxSeries, err := idx("SERIES")
	if err != nil {
		return nil, nil, err
	}

	symbolToID := make(map[string]int)
	idToSymbol := make(map[int]string)

	rowNo := 1
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, nil, fmt.Errorf("read dhan csv row %d: %w", rowNo, err)
		}

		rowNo++

		if row[idxSeries] != "EQ" && row[idxSeries] != "BE" {
			continue
		}

		symbol := strings.TrimSpace(row[idxSymbol])
		id, _ := strconv.Atoi(row[idxSecurityID])

		symbolToID[symbol] = id
		idToSymbol[id] = symbol
	}

	return symbolToID, idToSymbol, nil
}
