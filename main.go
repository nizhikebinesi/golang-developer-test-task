package main

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang-developer-test-task/infrastructure/redclient"
	"net/http"
	"strconv"
	"time"

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
			Status:         200,
		}

		next.ServeHTTP(recorder, r)

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

	dbLogic := NewDBProcessor(client, logger)
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
