package main

import (
	"context"
	"golang-developer-test-task/infrastructure/redclient"
	"golang-developer-test-task/structs"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/singleflight"

	// https://github.com/uber-go/automaxprocs
	// _ "go.uber.org/automaxprocs"
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

// func init() {
//	prometheus.MustRegister(statusCounter)
//	prometheus.MustRegister(timings)
//	prometheus.MustRegister(counter)
//}

// StatusRecorder saves status from http.ResponseWriter
type StatusRecorder struct {
	http.ResponseWriter
	Status int
}

// WriteHeader saves status for further use
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
			Observe(time.Since(start).Seconds())
		counter.
			WithLabelValues(r.URL.Path).
			Inc()
	})
}

// Gzip Compression
// type gzipResponseWriter struct {
//	io.Writer
//	http.ResponseWriter
//}
//
// func (w gzipResponseWriter) Write(b []byte) (int, error) {
//	return w.Writer.Write(b)
//}

// func Gzip(handler http.Handler) http.Handler {
//	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
//			handler.ServeHTTP(w, r)
//			return
//		}
//		w.Header().Set("Content-Encoding", "gzip")
//		gz := gzip.NewWriter(w)
//		defer gz.Close()
//		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
//		handler.ServeHTTP(gzw, r)
//	})
// }

func main() {
	runtime.GOMAXPROCS(2)

	prometheus.MustRegister(statusCounter)
	prometheus.MustRegister(timings)
	prometheus.MustRegister(counter)

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

	s := &singleflight.Group{}

	timeout := 5 * time.Minute

	cache := ttlcache.New[string, structs.PaginationObject](
		ttlcache.WithTTL[string, structs.PaginationObject](timeout))
	go cache.Start()

	// respCache := ttlcache.New[string, string](
	//	ttlcache.WithTTL[string, string](timeout))
	// go respCache.Start()

	// var pool = &sync.Pool{
	//	New: func() interface{} {
	//		s := structs.SearchObject{}
	//		return &s
	//	}}
	// var pool1 = &sync.Pool{
	//	New: func() interface{} {
	//		s := structs.PaginationObject{}
	//		return &s
	//	},
	//}

	// dbLogic := NewDBProcessor(client, logger, s, cache, pool, pool1)
	dbLogic := NewDBProcessor(client, logger, s, cache)
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/api/load_file", dbLogic.HandleLoadFile)

	mux.HandleFunc("/api/load_from_url", dbLogic.HandleLoadFromURL)

	mux.HandleFunc("/api/load_from_json", dbLogic.HandleLoadJSON)

	//https://nimblehq.co/blog/getting-started-with-redisearch
	mux.HandleFunc("/api/search", dbLogic.HandleSearch)

	mux.HandleFunc("/", dbLogic.HandleMainPage)

	wrappedHandler := timeTrackingMiddleware(mux)
	// wrappedHandler := Gzip(timeTrackingMiddleware(mux))
	// wrappedHandler := timeTrackingMiddleware(Gzip(mux))

	err = http.ListenAndServe(":"+port, wrappedHandler)
	if err != nil {
		panic(err)
	}
}
