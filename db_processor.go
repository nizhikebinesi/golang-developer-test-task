package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jellydator/ttlcache/v3"
	jsoniter "github.com/json-iterator/go"
	"golang-developer-test-task/infrastructure/redclient"
	"golang-developer-test-task/structs"
	"golang.org/x/sync/singleflight"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.uber.org/zap"
)

type (
	jsonObjectsProcessorFunc func(io.Reader) error

	// DBProcessor needs for dependency injection
	DBProcessor struct {
		client        *redclient.RedisClient
		logger        *zap.Logger
		jsonProcessor jsonObjectsProcessorFunc
		group         *singleflight.Group
		cache         *ttlcache.Cache[string, structs.PaginationObject]
		respCache     *ttlcache.Cache[string, string]
	}

	// Handler is type for handler function
	Handler func(http.ResponseWriter, *http.Request)

	infoProcessor func(structs.Info)
)

// NewDBProcessor is a constructor for creating basic version of DBProcessor
func NewDBProcessor(client *redclient.RedisClient, logger *zap.Logger,
	group *singleflight.Group, cache *ttlcache.Cache[string, structs.PaginationObject]) *DBProcessor {
	d := &DBProcessor{}
	d.client = client
	d.logger = logger
	d.group = group
	d.cache = cache
	d.jsonProcessor = func(prc infoProcessor) jsonObjectsProcessorFunc {
		return func(reader io.Reader) error {
			return d.processJSONs(reader, prc)
		}
	}(d.saveInfo)
	return d
}

// saveInfo is method for info saving to DB
func (d *DBProcessor) saveInfo(info structs.Info) {
	err := d.client.AddValue(context.Background(), info)
	if err != nil && err != redis.Nil {
		d.logger.Error("error inside processJSONs in goroutine",
			zap.Error(err))
		return
	}
}

// processJSONs read jsons from reader and write it to Redis client
func (d *DBProcessor) processJSONs(reader io.Reader, processor infoProcessor) (err error) {
	//out, err := io.ReadAll(reader)
	//if err != nil {
	//	d.logger.Error("error inside processJSONs during ReadAll",
	//		zap.Error(err))
	//	return err
	//}
	dec := json.NewDecoder(reader)

	_, err = dec.Token()
	if err != nil {
		d.logger.Error("error inside processJSONs during decoding the first token in stream",
			zap.Error(err))
		return err
	}
	//for dec.More() {
	//	var info structs.Info
	//	err = dec.Decode()
	//}

	var infoList = make(structs.InfoList, 0)
	for dec.More() {
		var info structs.Info
		err = dec.Decode(&info)
		if err != nil {
			d.logger.Error("error inside processJSONs during decoding stream")
			return err
		}
	}
	//err = jsoniter.Unmarshal(out, &infoList)
	//if err != nil {
	//	d.logger.Error("error inside processJSONs during Unmarshal",
	//		zap.Error(err))
	//	return err
	//}
	_, err = dec.Token()
	if err != nil {
		d.logger.Error("error inside processJSONs during decoding the last token in stream",
			zap.Error(err))
		return err
	}
	for _, info := range infoList {
		go processor(info)
	}
	return nil
}

// processFileFromURL handle json file from URL
func (d *DBProcessor) processFileFromURL(url string, processor jsonObjectsProcessorFunc) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		d.logger.Error("error during make NewRequest in processFileFromURL", zap.Error(err))
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	client := http.Client{
		Timeout: 30 * time.Second,
	}
	resp, err := client.Do(req)
	d.logger.Info("AAA!")
	if err != nil {
		d.logger.Error("error inside processFileFromURL in singleflight", zap.Error(err))
		return err
	}
	if resp.ContentLength > 32<<20 {
		d.logger.Error("too big resp body", zap.Int64("content_length", resp.ContentLength))
		return errors.New("too big resp body in processFileFromURL")
	}
	if contentType := resp.Header.Get("Content-Type"); contentType != "application/json" && contentType != "application/octet-stream" {
		d.logger.Error("unsupported Content-Type", zap.String("content_type", contentType))
		return errors.New("unsupported Content-Type")
	}
	err = processor(resp.Body)
	return err
}

// processFileFromRequest handle json file from request
func (d *DBProcessor) processFileFromRequest(r *http.Request, fileName string, processor jsonObjectsProcessorFunc) (err error) {
	file, _, err := r.FormFile(fileName)
	if err != nil {
		d.logger.Error("error inside processFileFromRequest",
			zap.Error(err))
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	err = processor(file)
	return err
}

// methodMiddleware is a function to return wrapped handler
func (d *DBProcessor) methodMiddleware(handler Handler, validMethod string) Handler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != validMethod {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		handler(w, r)
	}
}

// HandleLoadFile is handler for /api/load_file
func (d *DBProcessor) HandleLoadFile(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		d.logger.Error("error during file parsing in HandleLoadFile", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = d.processFileFromRequest(r, "uploadFile", d.jsonProcessor)
	if err != nil {
		d.logger.Error("error during file processing in HandleLoadFile", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleLoadFile is handler for /api/load_json
func (d *DBProcessor) HandleLoadJSON(w http.ResponseWriter, r *http.Request) {
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		d.logger.Error("error during using jsonProcessor in HandleLoadJSON", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var infoList structs.InfoList
	err = jsoniter.Unmarshal(bs, &infoList)
	if err != nil {
		d.logger.Error("error during Unmarshal in HandleLoadJSON", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	for _, info := range infoList {
		go d.saveInfo(info)
	}
	w.WriteHeader(http.StatusOK)
}

// HandleLoadFromURL is handler for /api/load_from_url
func (d *DBProcessor) HandleLoadFromURL(w http.ResponseWriter, r *http.Request) {
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		d.logger.Error("during ReadAll in HandleLoadFromURL")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var urlObj structs.URLObject
	err = jsoniter.Unmarshal(bs, &urlObj)
	if err != nil {
		d.logger.Error("during Unmarshal in HandleLoadFromURL")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if _, err := url.Parse(urlObj.URL); err != nil {
		d.logger.Error("during url parsing in HandleLoadFromURL")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = d.processFileFromURL(urlObj.URL, d.jsonProcessor)
	if err != nil {
		d.logger.Error("error during file processing from url", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleSearch is handler for /api/search
func (d *DBProcessor) HandleSearch(w http.ResponseWriter, r *http.Request) {
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		d.logger.Error("during ReadAll", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var searchObj structs.SearchObject
	err = jsoniter.Unmarshal(bs, &searchObj)
	if err != nil {
		d.logger.Error("during Unmarshal",
			zap.Error(err),
			zap.String("searchObj", fmt.Sprintf("%v", searchObj)))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	searchStr := ""
	multiple := false
	switch {
	case searchObj.SystemObjectID != nil:
		searchStr = *searchObj.SystemObjectID
	case searchObj.GlobalID != nil:
		searchStr = fmt.Sprintf("global_id:%d", *searchObj.GlobalID)
	case searchObj.ID != nil:
		searchStr = fmt.Sprintf("id:%d", *searchObj.ID)
	case searchObj.IDEn != nil:
		searchStr = fmt.Sprintf("id_en:%d", *searchObj.IDEn)
	case searchObj.Mode != nil:
		searchStr = fmt.Sprintf("mode:%s", *searchObj.Mode)
		multiple = true
	case searchObj.ModeEn != nil:
		searchStr = fmt.Sprintf("mode_en:%s", *searchObj.ModeEn)
		multiple = true
	default:
		d.logger.Error("searchObj'group all necessary fields are nil")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	result, err, _ := d.group.Do(searchStr, func() (interface{}, error) {
		// TODO: add changing cache on insert to Redis(with condition)
		item := d.cache.Get(searchStr)
		if item != nil {
			return item.Value(), nil
		}

		ctx := context.Background()
		paginationObj := structs.PaginationObject{}
		paginationObj.Offset = int64(searchObj.Offset)
		var paginationSize int64 = 5
		infoList, totalSize, err := d.client.FindValues(
			ctx, searchStr, multiple, paginationSize,
			paginationObj.Offset)
		if err != nil && err != redis.Nil {
			d.logger.Error("during search in DB in singleflight", zap.Error(err))
			return paginationObj, err
		}
		paginationObj.Size = totalSize
		paginationObj.Data = infoList

		d.cache.Set(searchStr, paginationObj, ttlcache.DefaultTTL)

		return paginationObj, nil
	})
	if err != nil {
		d.logger.Error("during search in DB", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	paginationObj := result.(structs.PaginationObject)

	bs, _ = jsoniter.Marshal(paginationObj)
	w.Header().Set("Content-Type", "application/json; charset=windows-1251")
	_, _ = w.Write(bs)
}

// HandleMainPage is handler for main page
func (d *DBProcessor) HandleMainPage(w http.ResponseWriter, r *http.Request) {
	tmp := time.Now().Unix()
	h := md5.New()
	_, _ = io.WriteString(h, strconv.FormatInt(tmp, 10))
	token := fmt.Sprintf("%x", h.Sum(nil))
	t, _ := template.ParseFiles("static/index.tmpl")
	_ = t.Execute(w, token)
}
