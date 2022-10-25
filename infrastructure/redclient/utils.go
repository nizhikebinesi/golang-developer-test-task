package redclient

import (
	"context"
	"errors"
	"fmt"
	"golang-developer-test-task/structs"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/mailru/easyjson"

	"github.com/go-redis/redis/v8"
)

// AddValue add info to Redis storage
func (r *RedisClient) AddValue(ctx context.Context, info structs.Info) (err error) {
	bs, _ := jsoniter.Marshal(info)

	globalID := fmt.Sprintf("global_id:%d", info.GlobalID)
	id := fmt.Sprintf("id:%d", info.ID)
	idEn := fmt.Sprintf("id_en:%d", info.IDEn)
	mode := fmt.Sprintf("mode:%s", info.Mode)
	modeEn := fmt.Sprintf("mode_en:%s", info.ModeEn)

	txf := func(tx *redis.Tx) error {
		err := tx.Get(ctx, info.SystemObjectID).Err()
		if err != nil && err != redis.Nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, info.SystemObjectID, bs, 0)
			pipe.Set(ctx, globalID, info.SystemObjectID, 0)
			pipe.Set(ctx, id, info.SystemObjectID, 0)
			pipe.Set(ctx, idEn, info.SystemObjectID, 0)
			pipe.RPush(ctx, mode, info.SystemObjectID)
			pipe.RPush(ctx, modeEn, info.SystemObjectID)
			return nil
		})
		return err
	}

	for i := 0; i < r.MaxRetries; i++ {
		err = r.Watch(ctx, txf, info.SystemObjectID, globalID, id, idEn, mode, modeEn)
		if !errors.Is(err, redis.TxFailedErr) {
			// if err != redis.TxFailedErr {
			// fmt.Printf("%v ahaha", err)
			return err
		}
	}
	return err
}

// AddValues add infos to Redis storage
func (r *RedisClient) AddValues(ctx context.Context, infos structs.InfoList) (err error) {
	if len(infos) == 0 {
		return
	}
	size := len(infos)
	bss := make([][]byte, size)
	systemIDs := make([]string, size)
	globalIDs := make([]string, size)
	ids := make([]string, size)
	idEns := make([]string, size)
	modes := make([]string, size)
	modeEns := make([]string, size)
	keys := make([]string, 0, size*6)

	for i := range bss {
		bss[i], _ = jsoniter.Marshal(infos[i])
		globalID := fmt.Sprintf("global_id:%d", infos[i].GlobalID)
		id := fmt.Sprintf("id:%d", infos[i].ID)
		idEn := fmt.Sprintf("id_en:%d", infos[i].IDEn)
		mode := fmt.Sprintf("mode:%s", infos[i].Mode)
		modeEn := fmt.Sprintf("mode_en:%s", infos[i].ModeEn)
		systemIDs[i] = infos[i].SystemObjectID
		globalIDs[i] = globalID
		ids[i] = id
		idEns[i] = idEn
		modes[i] = mode
		modeEns[i] = modeEn
		keys = append(keys, systemIDs[i])
		keys = append(keys, globalID)
		keys = append(keys, id)
		keys = append(keys, idEn)
		keys = append(keys, mode)
		keys = append(keys, modeEn)
	}

	txf := func(tx *redis.Tx) error {
		for _, systemID := range systemIDs {
			err := tx.Get(ctx, systemID).Err()
			if err != nil && err != redis.Nil {
				return err
			}
		}

		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for i := range bss {
				pipe.Set(ctx, systemIDs[i], bss[i], 0)
				pipe.Set(ctx, globalIDs[i], systemIDs[i], 0)
				pipe.Set(ctx, ids[i], systemIDs[i], 0)
				pipe.Set(ctx, idEns[i], systemIDs[i], 0)
				pipe.RPush(ctx, modes[i], systemIDs[i])
				pipe.RPush(ctx, modeEns[i], systemIDs[i])
			}
			return nil
		})
		return err
	}

	for i := 0; i < r.MaxRetries; i++ {
		err = r.Watch(ctx, txf, keys...)
		if !errors.Is(err, redis.TxFailedErr) {
			// if err != redis.TxFailedErr {
			// fmt.Printf("%v ahaha", err)
			return err
		}
	}
	return err
}

// FindValues is a method for searching values by searchStr
func (r *RedisClient) FindValues(ctx context.Context, searchStr string, multiple bool, paginationSize, offset int64) (infoList structs.InfoList, totalSize int64, err error) {
	if !multiple {
		v, err := r.Get(ctx, searchStr).Result()
		if err != nil {
			return infoList, 0, err
		}
		if strings.Contains(searchStr, ":") {
			v, err = r.Get(ctx, v).Result()
			if err != nil {
				return infoList, 0, err
			}
		}
		var info structs.Info
		err = jsoniter.Unmarshal([]byte(v), &info)
		if err != nil {
			return infoList, 1, err
		}
		infoList = append(infoList, info)
		return infoList, 1, nil
	}

	size, err := r.LLen(ctx, searchStr).Result()
	if err != nil {
		return infoList, 0, err
	}

	if paginationSize <= 0 {
		return infoList, size, nil
	}
	start := offset
	end := offset + paginationSize
	if start > size {
		return infoList, size, nil
	}

	var vs []string
	vs, err = r.LRange(ctx, searchStr, start, end).Result()
	if err != nil {
		return infoList, size, err
	}

	for _, v := range vs {
		var info structs.Info
		vv, err := r.Get(ctx, v).Result()
		if err != nil {
			return infoList, size, err
		}
		err = easyjson.Unmarshal([]byte(vv), &info)
		if err != nil {
			return infoList, size, err
		}
		infoList = append(infoList, info)
	}
	return infoList, size, nil
}
