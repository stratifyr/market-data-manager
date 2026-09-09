package dataproviders

import (
	"errors"
	"fmt"
	"time"

	"gofr.dev/pkg/gofr"
)

type Provider interface {
	EquityLTP(ctx *gofr.Context, symbols []string) (map[string]float64, error)
	EquityOHLCV(ctx *gofr.Context, symbols []string) (map[string]*OHLCVData, error)
	EquityHistoricalOHLCV(ctx *gofr.Context, symbol string, startDate, endDate time.Time) ([]*HistoricalOHLCV, error)
	IndexValue(ctx *gofr.Context, indexNames []string) (map[string]float64, error)
	IndexOHLCV(ctx *gofr.Context, indexNames []string) (map[string]*OHLCVData, error)
	IndexHistoricalOHLCV(ctx *gofr.Context, symbol string, startDate, endDate time.Time) ([]*HistoricalOHLCV, error)
}

func New(app *gofr.App) (Provider, error) {
	switch app.Config.Get("MARKET_DATA_PROVIDER") {
	case "DHAN_MARKET_API":
		return NewDhanHQClient(app)
	default:
		return nil, errors.New("invalid MARKET_DATA_PROVIDER")
	}
}

type OHLCVData struct {
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume int
}

type HistoricalOHLCV struct {
	Date time.Time
	*OHLCVData
}

func (o OHLCVData) String() string {
	return fmt.Sprintf("{o=%0.2f, h=%0.2f, l=%0.2f, c=%0.2f, v=%d}", o.Open, o.High, o.Low, o.Close, o.Volume)
}
