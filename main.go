package main

import (
	"context"
	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang-developer-test-task/infrastructure/redclient"
	"golang-developer-test-task/structs"
	"golang.org/x/sync/singleflight"
	"net/http"
	"strconv"
	"time"

	// https://github.com/uber-go/automaxprocs
	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"
)

var (
	statusCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "method_status_counter",
			Help: "Per method status counter",
		},
		[]string{"method", "status"})
	timings = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "method_timing",
			Help: "Per method timing",
		},
		[]string{"method"})
	counter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "method_counter",
			Help: "Per method counter",
		},
		[]string{"method"})
)

func init() {
	prometheus.MustRegister(statusCounter)
	prometheus.MustRegister(timings)
	prometheus.MustRegister(counter)
}

type StatusRecorder struct {
	http.ResponseWriter
	Status int
}

func (r *StatusRecorder) WriteHeader(status int) {
	r.Status = status
	r.ResponseWriter.WriteHeader(status)
}

func timeTrackingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &StatusRecorder{
			ResponseWriter: w,
			Status:         http.StatusOK,
		}

		next.ServeHTTP(recorder, r)

		// TODO
		// r.URL.Path приходит от юзера! не делайте так в проде!
		status := strconv.Itoa(recorder.Status)
		statusCounter.WithLabelValues(r.URL.Path, status).
			Inc()
		timings.
			WithLabelValues(r.URL.Path).
			Observe(float64(time.Since(start).Seconds()))
		counter.
			WithLabelValues(r.URL.Path).
			Inc()
	})
}

func main() {
	//runtime.GOMAXPROCS(4)
	port := "8080"

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() {
		err = logger.Sync()
	}()

	ctx := context.Background()
	conf := redclient.RedisConfig{}
	conf.Load()

	client := redclient.NewRedisClient(ctx, conf)
	defer func() {
		err = client.Close()
		if err != nil {
			panic(err)
		}
	}()

	s := singleflight.Group{}

	cache := ttlcache.New[string, structs.PaginationObject](
		ttlcache.WithTTL[string, structs.PaginationObject](5 * time.Minute))
	go cache.Start()

	dbLogic := NewDBProcessor(client, logger, &s, cache)
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/api/load_file", dbLogic.HandleLoadFile)

	mux.HandleFunc("/api/load_from_url", dbLogic.HandleLoadFromURL)

	//https://nimblehq.co/blog/getting-started-with-redisearch
	mux.HandleFunc("/api/search", dbLogic.HandleSearch)

	mux.HandleFunc("/", dbLogic.HandleMainPage)

	wrappedHandler := timeTrackingMiddleware(mux)

	err = http.ListenAndServe(":"+port, wrappedHandler)
	if err != nil {
		panic(err)
	}
}
