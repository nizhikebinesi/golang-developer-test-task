package redclient

import (
	"context"
	"fmt"
	"golang-developer-test-task/structs"

	"github.com/mailru/easyjson"
)

// AddValue add info to Redis storage
func (r *RedisClient) AddValue(ctx context.Context, info structs.Info) (err error) {
	// TODO: add transactions - https://stackoverflow.com/questions/40999446/how-to-create-redis-transaction-in-go-using-go-redis-redis-package
	bs, err := easyjson.Marshal(info)
	if err != nil {
		return err
	}
	err = r.Set(ctx, info.SystemObjectID, bs, 0).Err()
	if err != nil {
		return err
	}
	globalID := fmt.Sprintf("global_id:%d", info.GlobalID)
	err = r.Set(ctx, globalID, info.SystemObjectID, 0).Err()
	if err != nil {
		return err
	}
	id := fmt.Sprintf("id:%d", info.ID)
	err = r.Set(ctx, id, info.SystemObjectID, 0).Err()
	if err != nil {
		return err
	}
	idEn := fmt.Sprintf("id_en:%d", info.IDEn)
	err = r.Set(ctx, idEn, info.SystemObjectID, 0).Err()
	if err != nil {
		return err
	}
	mode := fmt.Sprintf("mode:%s", info.Mode)
	err = r.RPush(ctx, mode, info.SystemObjectID).Err()
	if err != nil {
		return err
	}
	modeEn := fmt.Sprintf("mode_en:%s", info.ModeEn)
	err = r.RPush(ctx, modeEn, info.SystemObjectID).Err()
	if err != nil {
		return err
	}
	return nil
}

// FindValues is a method for searching values by searchStr
func (r *RedisClient) FindValues(ctx context.Context, searchStr string, multiple bool, paginationSize, offset int64) (infoList structs.InfoList, totalSize int64, err error) {
	if !multiple {
		v, err := r.Get(ctx, searchStr).Result()
		if err != nil {
			return infoList, 0, err
		}
		var info structs.Info
		err = easyjson.Unmarshal([]byte(v), &info)
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
		err = easyjson.Unmarshal([]byte(v), &info)
		if err != nil {
			return
		}
		infoList = append(infoList, info)
	}
	return infoList, size, nil
}
