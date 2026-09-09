package dataproviders

import (
	"errors"
	"fmt"
	"time"

	"gofr.dev/pkg/gofr"
)

type Provider interface {
	LTP(ctx *gofr.Context, symbols []string) (map[string]float64, error)
	Volume(ctx *gofr.Context, symbols []string) (map[string]int, error)
	OHLC(ctx *gofr.Context, symbols []string) (map[string]*OHLCData, error)
	HistoricalOHLC(ctx *gofr.Context, symbol string, startDate, endDate time.Time) ([]*HistoricalOHLC, error)
	IndexValue(ctx *gofr.Context, indexNames []string) (map[string]float64, error)
	IndexOHLC(ctx *gofr.Context, indexNames []string) (map[string]*OHLCData, error)
	IndexHistoricalOHLC(ctx *gofr.Context, symbol string, startDate, endDate time.Time) ([]*HistoricalOHLC, error)
}

func New(app *gofr.App) (Provider, error) {
	switch app.Config.Get("MARKET_DATA_PROVIDER") {
	case "DHAN_MARKET_API":
		return NewDhanHQClient(app)
	default:
		return nil, errors.New("invalid MARKET_DATA_PROVIDER")
	}
}

type OHLCData struct {
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume int
}

type HistoricalOHLC struct {
	Date time.Time
	*OHLCData
}

func (o OHLCData) String() string {
	return fmt.Sprintf("{o=%0.2f, h=%0.2f, l=%0.2f, c=%0.2f, v=%d}", o.Open, o.High, o.Low, o.Close, o.Volume)
}
