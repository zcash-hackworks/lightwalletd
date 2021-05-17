package common

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	// Map of all historical prices. Date as "yyyy-mm-dd" to price in cents
	historicalPrices map[string]float64 = make(map[string]float64)

	// The latest price
	latestPrice float64 = -1

	// Latest price was fetched at
	latestPriceAt time.Time

	// Mutex to control both historical and latest price
	pricesRwMutex sync.RWMutex

	// Full path of the persistence file
	pricesFileName string
)

func fetchAPIPrice(url string, resultPath []string) (float64, error) {
	resp, err := http.Get(url)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1, err
	}

	var priceJSON map[string]interface{}
	json.Unmarshal(body, &priceJSON)

	for i := 0; i < len(resultPath); i++ {
		d, ok := priceJSON[resultPath[i]]
		if !ok {
			return -1, fmt.Errorf("API error: couldn't find '%s'", resultPath[i])
		}

		switch v := d.(type) {
		case float64:
			return v, nil
		case string:
			{
				price, err := strconv.ParseFloat(v, 64)
				return price, err
			}

		case map[string]interface{}:
			priceJSON = v
		}

	}

	return -1, errors.New("path didn't result in lookup")
}

func fetchCoinbasePrice() (float64, error) {
	return fetchAPIPrice("https://api.coinbase.com/v2/exchange-rates?currency=ZEC", []string{"data", "rates", "USD"})

}

func fetchCoinCapPrice() (float64, error) {
	return fetchAPIPrice("https://api.coincap.io/v2/rates/zcash", []string{"data", "rateUsd"})
}

func fetchBinancePrice() (float64, error) {
	return fetchAPIPrice("https://api.binance.com/api/v3/avgPrice?symbol=ZECUSDC", []string{"price"})
}

func fetchHistoricalCoingeckoPrice(ts *time.Time) (float64, error) {
	dt := ts.Format("02-01-2006") // dd-mm-yyyy
	url := fmt.Sprintf("https://api.coingecko.com/api/v3/coins/zcash/history?date=%s?id=zcash", dt)

	return fetchAPIPrice(url, []string{"market_data", "current_price", "usd"})
}

// Median gets the median number in a slice of numbers
func median(inp []float64) (median float64) {

	// Start by sorting a copy of the slice
	sort.Float64s(inp)

	// No math is needed if there are no numbers
	// For even numbers we add the two middle numbers
	// and divide by two using the mean function above
	// For odd numbers we just use the middle number
	l := len(inp)
	if l == 0 {
		return -1
	} else if l == 2 {
		return (inp[0] + inp[1]) / 2
	} else if l%2 == 0 {
		return (inp[l/2-1] + inp[l/2+1]) / 2
	} else {
		return inp[l/2]
	}
}

// fetchPriceFromWebAPI will fetch prices from multiple places, discard outliers and return the
// concensus price
func fetchPriceFromWebAPI() (float64, error) {
	// We'll fetch prices from all our endpoints, and use the median price from that
	priceProviders := []func() (float64, error){fetchBinancePrice, fetchCoinCapPrice, fetchCoinbasePrice}

	ch := make(chan float64)

	// Get all prices
	for _, provider := range priceProviders {
		go func(provider func() (float64, error)) {
			price, err := provider()
			if err != nil {
				Log.WithFields(logrus.Fields{
					"method":   "CurrentPrice",
					"provider": runtime.FuncForPC(reflect.ValueOf(provider).Pointer()).Name(),
					"error":    err,
				}).Error("Service")

				ch <- -1
			} else {
				Log.WithFields(logrus.Fields{
					"method":   "CurrentPrice",
					"provider": runtime.FuncForPC(reflect.ValueOf(provider).Pointer()).Name(),
					"price":    price,
				}).Info("Service")

				ch <- price
			}
		}(provider)
	}

	prices := make([]float64, 0)
	for range priceProviders {
		price := <-ch
		if price > 0 {
			prices = append(prices, price)
		}
	}

	// sort
	sort.Float64s(prices)

	// Get the median price
	median1 := median(prices)

	// Discard all values that are more than 20% outside the median
	validPrices := make([]float64, 0)
	for _, price := range prices {
		if (math.Abs(price-median1) / median1) > 0.2 {
			Log.WithFields(logrus.Fields{
				"method": "CurrentPrice",
				"error":  fmt.Sprintf("Discarding price (%.2f) because too far away from median (%.2f", price, median1),
			}).Error("Service")
		} else {
			validPrices = append(validPrices, price)
		}
	}

	// If we discarded too many, return an error
	if len(validPrices) < (len(prices)/2 + 1) {
		return -1, errors.New("not enough valid prices")
	} else {
		return median(validPrices), nil
	}
}

func readHistoricalPricesFile() (map[string]float64, error) {
	f, err := os.Open(pricesFileName)
	if err != nil {
		Log.Errorf("Couldn't open file %s for writing: %v", pricesFileName, err)
		return nil, err
	}
	defer f.Close()

	j := gob.NewDecoder(f)
	var prices map[string]float64
	err = j.Decode(&prices)
	if err != nil {
		Log.Errorf("Couldn't decode historical prices: %v", err)
		return nil, err
	}

	Log.WithFields(logrus.Fields{
		"method":  "HistoricalPrice",
		"action":  "Read historical prices file",
		"records": len(prices),
	}).Info("Service")
	return prices, nil
}

func writeHistoricalPricesMap() {
	// Serialize the map to disk.
	f, err := os.OpenFile(pricesFileName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		Log.Errorf("Couldn't open file %s for writing: %v", pricesFileName, err)
		return
	}
	defer f.Close()

	j := gob.NewEncoder(f)

	{
		// Read lock
		pricesRwMutex.RLock()
		err = j.Encode(historicalPrices)
		pricesRwMutex.RUnlock()

		if err != nil {
			Log.Errorf("Couldn't encode historical prices: %v", err)
			return
		}
	}

	Log.WithFields(logrus.Fields{
		"method": "HistoricalPrice",
		"action": "Wrote historical prices file",
	}).Info("Service")
}

func GetCurrentPrice() (float64, error) {
	// Read lock
	pricesRwMutex.RLock()
	defer pricesRwMutex.RUnlock()

	// If the current price is too old, don't return it.
	if time.Since(latestPriceAt).Hours() > 3 {
		return -1, errors.New("price too old")
	}

	return latestPrice, nil
}

func writeLatestPrice(price float64) {
	{
		// Read lock
		pricesRwMutex.RLock()

		// Check if the time has "rolled over", if yes then preserve the last price
		// as the previous day's historical price
		if latestPrice > 0 && latestPriceAt.Format("2006-01-02") != time.Now().Format("2006-01-02") {
			// update the historical price.
			// First, make a copy of the time
			t := time.Unix(latestPriceAt.Unix(), 0)

			go addHistoricalPrice(latestPrice, &t)
		}
		pricesRwMutex.RUnlock()
	}

	// Write lock
	pricesRwMutex.Lock()

	latestPrice = price
	latestPriceAt = time.Now()

	pricesRwMutex.Unlock()
}

func GetHistoricalPrice(ts *time.Time) (float64, *time.Time, error) {
	dt := ts.Format("2006-01-02")
	canonicalTime, err := time.Parse("2006-01-02", dt)
	if err != nil {
		return -1, nil, err
	}

	{
		// Read lock
		pricesRwMutex.RLock()
		val, ok := historicalPrices[dt]
		pricesRwMutex.RUnlock()

		if ok {
			return val, &canonicalTime, nil
		}
	}

	{
		// Check if this is the same as the current latest price

		// Read lock
		pricesRwMutex.RLock()
		var price = latestPrice
		var returnPrice = price > 0 && latestPriceAt.Format("2006-01-02") == dt
		pricesRwMutex.RUnlock()

		if returnPrice {
			return price, &canonicalTime, nil
		}
	}

	// Fetch price from web API
	price, err := fetchHistoricalCoingeckoPrice(ts)
	if err != nil {
		Log.Errorf("Couldn't read historical prices from Coingecko: %v", err)
		return -1, nil, err
	}

	go addHistoricalPrice(price, ts)

	return price, &canonicalTime, err
}

func addHistoricalPrice(price float64, ts *time.Time) {
	if price <= 0 {
		return
	}
	dt := ts.Format("2006-01-02")

	// Read Lock
	pricesRwMutex.RLock()
	_, ok := historicalPrices[dt]
	pricesRwMutex.RUnlock()

	if !ok {
		// Write lock
		pricesRwMutex.Lock()
		historicalPrices[dt] = price
		defer pricesRwMutex.Unlock()

		go Log.WithFields(logrus.Fields{
			"method": "HistoricalPrice",
			"action": "Add",
			"date":   dt,
			"price":  price,
		}).Info("Service")
		go writeHistoricalPricesMap()
	}
}

// StartPriceFetcher starts a new thread that will fetch historical and current prices
func StartPriceFetcher(dbPath string, chainName string) {
	// Set the prices file name
	pricesFileName = filepath.Join(dbPath, chainName, "prices")

	// Read the historical prices if available
	if prices, err := readHistoricalPricesFile(); err != nil {
		Log.Errorf("Couldn't read historical prices, starting with empty map")
	} else {
		// Write lock
		pricesRwMutex.Lock()
		historicalPrices = prices
		pricesRwMutex.Unlock()
	}

	// Fetch the current price every 15 mins
	go func() {
		for {
			price, err := fetchPriceFromWebAPI()
			if err != nil {
				Log.Errorf("Error getting prices from web APIs: %v", err)
			} else {
				Log.WithFields(logrus.Fields{
					"method": "CurrentPrice",
					"price":  price,
				}).Info("Service")

				writeLatestPrice(price)
			}

			// Refresh every
			time.Sleep(15 * time.Minute)
		}
	}()
}
