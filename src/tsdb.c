/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "tsdb.h"

#include "config.h"
#include "consts.h"
#include "endianconv.h"
#include "filter_iterator.h"
#include "indexer.h"
#include "module.h"
#include "series_iterator.h"
#include "sample_iterator.h"
#include "multiseries_sample_iterator.h"
#include "multiseries_agg_dup_sample_iterator.h"
#include "rdb.h"

#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <assert.h> // assert
#include "rmutil/alloc.h"
#include "rmutil/logging.h"
#include "rmutil/strings.h"

static RedisModuleString *renameFromKey = NULL;

void deleteReferenceToDeletedSeries(RedisModuleCtx *ctx, Series *series) {
    Series *_series;
    RedisModuleKey *_key;
    int status;

    // 如果当前key存在源key，意味着当前key是一个压缩key
    if (series->srcKey) {
        status = GetSeries(ctx, series->srcKey, &_key, &_series, REDISMODULE_READ, false, true);
        // 如果源key中有和当前key同名的映射关系，则应该取消掉当前的映射关系
        if (!status || (!GetRule(_series->rules, series->keyName))) {
            SeriesDeleteSrcRule(series, series->srcKey);
        }
        if (status) {
            RedisModule_CloseKey(_key);
        }
    }

    CompactionRule *rule = series->rules;
    while (rule) {
        // 检查当前key对应的规则key信息
        // 如果存在映射关系不匹配的情况，则将对应的映射关系取消掉
        CompactionRule *nextRule = rule->nextRule;
        status = GetSeries(ctx, rule->destKey, &_key, &_series, REDISMODULE_READ, false, true);
        if (!status || !_series->srcKey ||
            (RedisModule_StringCompare(_series->srcKey, series->keyName) != 0)) {
            SeriesDeleteRule(series, rule->destKey);
        }
        if (status) {
            RedisModule_CloseKey(_key);
        }
        rule = nextRule;
    }
}

CompactionRule *GetRule(CompactionRule *rules, RedisModuleString *keyName) {
    CompactionRule *rule = rules;
    while (rule != NULL) {
        if (RedisModule_StringCompare(rule->destKey, keyName) == 0) {
            return rule;
        }
        rule = rule->nextRule;
    }
    return NULL;
}

int GetSeries(RedisModuleCtx *ctx,
              RedisModuleString *keyName,
              RedisModuleKey **key,
              Series **series,
              int mode,
              bool shouldDeleteRefs,
              bool isSilent) {
    if (shouldDeleteRefs) {
        mode = mode | REDISMODULE_WRITE;
    }

    // 存储series的时候，key是对应的时序的key，value是series结构
    RedisModuleKey *new_key = RedisModule_OpenKey(ctx, keyName, mode);
    if (RedisModule_KeyType(new_key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(new_key);
        if (!isSilent) {
            RTS_ReplyGeneralError(ctx, "TSDB: the key does not exist");
        }
        return FALSE;
    }
    if (RedisModule_ModuleTypeGetType(new_key) != SeriesType) {
        RedisModule_CloseKey(new_key);
        if (!isSilent) {
            RTS_ReplyGeneralError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        return FALSE;
    }

    *series = RedisModule_ModuleTypeGetValue(new_key);
    *key = new_key;

    if (shouldDeleteRefs) {
        deleteReferenceToDeletedSeries(ctx, *series);
    }

    return TRUE;
}

int dictOperator(RedisModuleDict *d, void *chunk, timestamp_t ts, DictOp op) {
    timestamp_t rax_key = htonu64(ts);
    switch (op) {
        case DICT_OP_SET:
            return RedisModule_DictSetC(d, &rax_key, sizeof(rax_key), chunk);
        case DICT_OP_REPLACE:
            return RedisModule_DictReplaceC(d, &rax_key, sizeof(rax_key), chunk);
        case DICT_OP_DEL:
            return RedisModule_DictDelC(d, &rax_key, sizeof(rax_key), NULL);
    }
    chunk = NULL;
    return REDISMODULE_OK; // silence compiler
}

Series *NewSeries(RedisModuleString *keyName, CreateCtx *cCtx) {
    Series *newSeries = (Series *)calloc(1, sizeof(Series));
    newSeries->keyName = keyName;
    // 创建的chunks是一个rax的dict
    // 其中key为时间戳，val为对应的时序格式的数据
    newSeries->chunks = RedisModule_CreateDict(NULL);
    newSeries->chunkSizeBytes = cCtx->chunkSizeBytes;
    newSeries->retentionTime = cCtx->retentionTime;
    newSeries->srcKey = NULL;
    newSeries->rules = NULL;
    newSeries->lastTimestamp = 0;
    newSeries->lastValue = 0;
    newSeries->totalSamples = 0;
    newSeries->labels = cCtx->labels;
    newSeries->labelsCount = cCtx->labelsCount;
    newSeries->options = cCtx->options;
    newSeries->duplicatePolicy = cCtx->duplicatePolicy;

    if (newSeries->options & SERIES_OPT_UNCOMPRESSED) {
        newSeries->options |= SERIES_OPT_UNCOMPRESSED;
        newSeries->funcs = GetChunkClass(CHUNK_REGULAR);
    } else {
        newSeries->options |= SERIES_OPT_COMPRESSED_GORILLA;
        newSeries->funcs = GetChunkClass(CHUNK_COMPRESSED);
    }

    if (!cCtx->skipChunkCreation) {
        Chunk_t *newChunk = newSeries->funcs->NewChunk(newSeries->chunkSizeBytes);
        dictOperator(newSeries->chunks, newChunk, 0, DICT_OP_SET);
        newSeries->lastChunk = newChunk;
    } else {
        newSeries->lastChunk = NULL;
    }

    return newSeries;
}

void SeriesTrim(Series *series, timestamp_t startTs, timestamp_t endTs) {
    // if not causedByRetention, caused by ts.del
    if (series->retentionTime == 0) {
        return;
    }

    // start iterator from smallest key
    // 从最小的key开始迭代，chunks 是一个dict
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);

    Chunk_t *currentChunk;
    void *currentKey;
    size_t keyLen;
    // 计算一个最小的时间戳
    // TODO: 这里的 series->retentionTime 应该是一个时间差的值，
    // 仿佛是代表着要保留的时间的范围，而不是一个具体的时间点
    // 那么 minTimestamp 就代表着最小的时间点
    timestamp_t minTimestamp = series->lastTimestamp > series->retentionTime
                                   ? series->lastTimestamp - series->retentionTime
                                   : 0;

    const ChunkFuncs *funcs = series->funcs;
    while ((currentKey = RedisModule_DictNextC(iter, &keyLen, (void *)&currentChunk))) {
        // 在遍历的过程中如果发现时间戳大于等于 minTimestamp ，则退出
        if (funcs->GetLastTimestamp(currentChunk) >= minTimestamp) {
            break;
        }

        // 如果当前的时间戳较小，我们需要删除对应的key
        RedisModule_DictDelC(series->chunks, currentKey, keyLen, NULL);
        // reseek iterator since we modified the dict,
        // go to first element that is bigger than current key
        // 重新设置迭代器，因为我们修改了dict
        // 转到大于当前键的第一个元素
        RedisModule_DictIteratorReseekC(iter, ">", currentKey, keyLen);

        // 删除掉对应key的数量
        // TODO: 为什么不是删除1，而是需要获取一下当前chunk的key的数量？？？
        // TODO: dcit中存储的是一个一个的chunk？每个chunk中可能有很多key？
        series->totalSamples -= funcs->GetNumOfSample(currentChunk);
        funcs->FreeChunk(currentChunk);
    }

    RedisModule_DictIteratorStop(iter);
}

// Encode timestamps as bigendian to allow correct lexical sorting
void seriesEncodeTimestamp(void *buf, timestamp_t timestamp) {
    uint64_t e;
    e = htonu64(timestamp);
    memcpy(buf, &e, sizeof(e));
}

void RestoreKey(RedisModuleCtx *ctx, RedisModuleString *keyname) {
    Series *series;
    RedisModuleKey *key = NULL;
    if (GetSeries(ctx, keyname, &key, &series, REDISMODULE_READ | REDISMODULE_WRITE, false, true) !=
        TRUE) {
        return;
    }

    // update self keyname cause on rdb_load we don't have the
    // key name and the key might be loaded with different name
    RedisModule_FreeString(NULL, series->keyName); // free old name allocated on rdb_load()
    RedisModule_RetainString(NULL, keyname);
    series->keyName = keyname;

    if (IsKeyIndexed(keyname)) {
        // Key is still in the index cause only free series being called, remove it for safety
        RemoveIndexedMetric(keyname);
    }
    IndexMetric(keyname, series->labels, series->labelsCount);

    if (last_rdb_load_version < TS_REPLICAOF_SUPPORT_VER) {
        // In versions greater than TS_REPLICAOF_SUPPORT_VER we delete the reference on the dump
        // stage

        // Remove references to other keys
        if (series->srcKey) {
            RedisModule_FreeString(NULL, series->srcKey);
            series->srcKey = NULL;
        }

        CompactionRule *rule = series->rules;
        while (rule != NULL) {
            CompactionRule *nextRule = rule->nextRule;
            FreeCompactionRule(rule);
            rule = nextRule;
        }
        series->rules = NULL;
    }

    RedisModule_CloseKey(key);
}

void IndexMetricFromName(RedisModuleCtx *ctx, RedisModuleString *keyname) {
    // Try to open the series
    Series *series;
    RedisModuleKey *key = NULL;
    RedisModuleString *_keyname = RedisModule_HoldString(ctx, keyname);
    const int status = GetSeries(ctx, _keyname, &key, &series, REDISMODULE_READ, false, true);
    if (!status) { // Not a timeseries key
        goto cleanup;
    }

    if (unlikely(IsKeyIndexed(_keyname))) {
        // when loading from rdb file the key shouldn't exist.
        size_t len;
        const char *str = RedisModule_StringPtrLen(_keyname, &len);
        RedisModule_Log(
            ctx, "warning", "Trying to load rdb a key=%s, which is already in index", str);
        RemoveIndexedMetric(_keyname); // for safety
    }

    IndexMetric(_keyname, series->labels, series->labelsCount);

cleanup:
    if (key) {
        RedisModule_CloseKey(key);
    }
    RedisModule_FreeString(ctx, _keyname);
}

void RenameSeriesFrom(RedisModuleCtx *ctx, RedisModuleString *key) {
    // keep in global variable for RenameSeriesTo() and increase recount
    RedisModule_RetainString(NULL, key);
    renameFromKey = key;
}

static void UpdateReferencesToRenamedSeries(RedisModuleCtx *ctx,
                                            Series *series,
                                            RedisModuleString *keyTo) {
    // A destination key was renamed
    if (series->srcKey) {
        Series *srcSeries;
        RedisModuleKey *srcKey;
        const int status =
            GetSeries(ctx, series->srcKey, &srcKey, &srcSeries, REDISMODULE_WRITE, false, false);
        if (status) {
            // Find the rule in the source key and rename the its destKey
            CompactionRule *rule = srcSeries->rules;
            while (rule) {
                if (RedisModule_StringCompare(renameFromKey, rule->destKey) == 0) {
                    RedisModule_FreeString(NULL, rule->destKey);
                    RedisModule_RetainString(NULL, keyTo);
                    rule->destKey = keyTo;
                    break; // Only one src can point back to destKey
                }
                rule = rule->nextRule;
            }
            RedisModule_CloseKey(srcKey);
        }
    }

    // A source key was renamed need to rename the srcKey on all the destKeys
    CompactionRule *rule = series->rules;
    while (rule) {
        Series *destSeries;
        RedisModuleKey *destKey;
        CompactionRule *nextRule = rule->nextRule; // avoid iterator invalidation
        const int status =
            GetSeries(ctx, rule->destKey, &destKey, &destSeries, REDISMODULE_WRITE, false, false);
        if (status) {
            // rename the srcKey in the destKey
            RedisModule_FreeString(NULL, destSeries->srcKey);
            RedisModule_RetainString(NULL, keyTo);
            destSeries->srcKey = keyTo;

            RedisModule_CloseKey(destKey);
        }
        rule = nextRule;
    }
}

void RenameSeriesTo(RedisModuleCtx *ctx, RedisModuleString *keyTo) {
    // Try to open the series
    Series *series;
    RedisModuleKey *key = NULL;
    const int status =
        GetSeries(ctx, keyTo, &key, &series, REDISMODULE_READ | REDISMODULE_WRITE, true, true);
    if (!status) { // Not a timeseries key
        goto cleanup;
    }

    // Reindex key by the new name
    RemoveIndexedMetric(renameFromKey);
    IndexMetric(keyTo, series->labels, series->labelsCount);

    UpdateReferencesToRenamedSeries(ctx, series, keyTo);

    RedisModule_FreeString(NULL, series->keyName);
    RedisModule_RetainString(NULL, keyTo);
    series->keyName = keyTo;

cleanup:
    if (key) {
        RedisModule_CloseKey(key);
    }
    RedisModule_FreeString(NULL, renameFromKey);
    renameFromKey = NULL;
}

void *CopySeries(RedisModuleString *fromkey, RedisModuleString *tokey, const void *value) {
    Series *src = (Series *)value;
    Series *dst = (Series *)calloc(1, sizeof(Series));
    memcpy(dst, src, sizeof(Series));
    RedisModule_RetainString(NULL, tokey);
    dst->keyName = tokey;

    // Copy labels
    if (src->labelsCount > 0) {
        dst->labels = calloc(src->labelsCount, sizeof(Label));
        for (size_t i = 0; i < dst->labelsCount; i++) {
            dst->labels[i].key = RedisModule_CreateStringFromString(NULL, src->labels[i].key);
            dst->labels[i].value = RedisModule_CreateStringFromString(NULL, src->labels[i].value);
        }
    }

    // Copy chunks
    dst->chunks = RedisModule_CreateDict(NULL);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(src->chunks, "^", NULL, 0);
    Chunk_t *curChunk;
    char *curKey;
    size_t keylen;
    while ((curKey = RedisModule_DictNextC(iter, &keylen, &curChunk)) != NULL) {
        Chunk_t *newChunk = src->funcs->CloneChunk(curChunk);
        RedisModule_DictSetC(dst->chunks, curKey, keylen, newChunk);
        if (src->lastChunk == curChunk) {
            dst->lastChunk = newChunk;
        }
    }

    RedisModule_DictIteratorStop(iter);

    dst->srcKey = NULL;
    dst->rules = NULL;

    RemoveIndexedMetric(tokey); // in case of replace
    if (dst->labelsCount > 0) {
        IndexMetric(tokey, dst->labels, dst->labelsCount);
    }
    return dst;
}

// Releases Series and all its compaction rules
// Doesn't free the cross reference between rules, only on "del" keyspace notification,
// since Flush anyway will free all series.
// Doesn't free the index just on "del" keyspace notification since RoF might delete the key while
// it's only on the disk, in this case FreeSeries won't be called just the "del" keyspace
// notification.
void FreeSeries(void *value) {
    Series *series = (Series *)value;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    Chunk_t *currentChunk;
    while (RedisModule_DictNextC(iter, NULL, (void *)&currentChunk) != NULL) {
        series->funcs->FreeChunk(currentChunk);
    }
    RedisModule_DictIteratorStop(iter);

    FreeLabels(series->labels, series->labelsCount);

    RedisModule_FreeDict(NULL, series->chunks);

    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        CompactionRule *nextRule = rule->nextRule;
        FreeCompactionRule(rule);
        rule = nextRule;
    }

    if (series->srcKey != NULL) {
        RedisModule_FreeString(NULL, series->srcKey);
    }
    if (series->keyName) {
        RedisModule_FreeString(NULL, series->keyName);
    }

    free(series);
}

void FreeCompactionRule(void *value) {
    CompactionRule *rule = (CompactionRule *)value;
    RedisModule_FreeString(NULL, rule->destKey);
    ((AggregationClass *)rule->aggClass)->freeContext(rule->aggContext);
    free(rule);
}

size_t SeriesGetChunksSize(Series *series) {
    size_t size = 0;
    Chunk_t *currentChunk;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, NULL, (void *)&currentChunk)) {
        size += series->funcs->GetChunkSize(currentChunk, true);
    }
    RedisModule_DictIteratorStop(iter);
    return size;
}

char *SeriesGetCStringLabelValue(const Series *series, const char *labelKey) {
    char *result = NULL;
    for (int i = 0; i < series->labelsCount; i++) {
        const char *currLabel = RedisModule_StringPtrLen(series->labels[i].key, NULL);
        if (strcmp(currLabel, labelKey) == 0) {
            result = strdup(RedisModule_StringPtrLen(series->labels[i].value, NULL));
            break;
        }
    }
    return result;
}

size_t SeriesMemUsage(const void *value) {
    Series *series = (Series *)value;

    size_t labelLen = 0;
    uint32_t labelsLen = 0;
    for (int i = 0; i < series->labelsCount; i++) {
        RedisModule_StringPtrLen(series->labels[i].key, &labelLen);
        labelsLen += (labelLen + 1);
        RedisModule_StringPtrLen(series->labels[i].value, &labelLen);
        labelsLen += (labelLen + 1);
    }

    size_t rulesSize = 0;
    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        rulesSize += sizeof(CompactionRule);
        rule = rule->nextRule;
    }

    return sizeof(series) + rulesSize + labelsLen + sizeof(Label) * series->labelsCount +
           SeriesGetChunksSize(series);
}

size_t SeriesGetNumSamples(const Series *series) {
    size_t numSamples = 0;
    if (series != NULL) {
        numSamples = series->totalSamples;
    }
    return numSamples;
}

int MultiSerieReduce(Series *dest,
                     Series **series,
                     size_t n_series,
                     const ReducerArgs *gropuByReducerArgs,
                     RangeArgs *args) {
    Sample sample;
    AbstractSampleIterator *iterator = MultiSeriesCreateAggDupSampleIterator(
        series, n_series, args, false, true, gropuByReducerArgs);
    while (iterator->GetNext(iterator, &sample) == CR_OK) {
        SeriesAddSample(dest, sample.timestamp, sample.value);
    }
    iterator->Close(iterator);
    return 1;
}

static bool RuleSeriesUpsertSample(RedisModuleCtx *ctx,
                                   Series *series,
                                   CompactionRule *rule,
                                   timestamp_t start,
                                   double val) {
    RedisModuleKey *key;
    Series *destSeries;
    if (!GetSeries(ctx,
                   rule->destKey,
                   &key,
                   &destSeries,
                   REDISMODULE_READ | REDISMODULE_WRITE,
                   false,
                   false)) {
        RedisModule_Log(ctx, "verbose", "%s", "Failed to retrieve downsample series");
        return false;
    }

    if (destSeries->totalSamples == 0) {
        SeriesAddSample(destSeries, start, val);
    } else {
        SeriesUpsertSample(destSeries, start, val, DP_LAST);
    }
    RedisModule_CloseKey(key);

    return true;
}

static void upsertCompaction(Series *series, UpsertCtx *uCtx) {
    if (series->rules == NULL) {
        return;
    }
    deleteReferenceToDeletedSeries(rts_staticCtx, series);
    CompactionRule *rule = series->rules;
    const timestamp_t upsertTimestamp = uCtx->sample.timestamp;
    const timestamp_t seriesLastTimestamp = series->lastTimestamp;
    while (rule != NULL) {
        const timestamp_t ruleTimebucket = rule->bucketDuration;
        const timestamp_t curAggWindowStart =
            CalcBucketStart(seriesLastTimestamp, ruleTimebucket, rule->timestampAlignment);
        const timestamp_t curAggWindowStartNormalized = BucketStartNormalize(curAggWindowStart);
        if (upsertTimestamp >= curAggWindowStartNormalized) {
            // upsert in latest timebucket
            const int rv = SeriesCalcRange(series,
                                           curAggWindowStartNormalized,
                                           curAggWindowStart + ruleTimebucket - 1,
                                           rule,
                                           NULL,
                                           NULL);
            if (rv == TSDB_ERROR) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }
        } else {
            const timestamp_t start =
                CalcBucketStart(upsertTimestamp, ruleTimebucket, rule->timestampAlignment);
            const timestamp_t startNormalized = BucketStartNormalize(start);
            // ensure last include/exclude
            double val = 0;
            const int rv = SeriesCalcRange(
                series, startNormalized, start + ruleTimebucket - 1, rule, &val, NULL);
            if (rv == TSDB_ERROR) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }

            if (!RuleSeriesUpsertSample(rts_staticCtx, series, rule, startNormalized, val)) {
                continue;
            }
        }
        rule = rule->nextRule;
    }
}

// update chunk in dictionary if first timestamp changed
static inline void update_chunk_in_dict(RedisModuleDict *chunks,
                                        Chunk_t *chunk,
                                        timestamp_t chunkOrigFirstTS,
                                        timestamp_t chunkFirstTSAfterOp) {
    if (dictOperator(chunks, NULL, chunkOrigFirstTS, DICT_OP_DEL) == REDISMODULE_ERR) {
        dictOperator(chunks, NULL, 0, DICT_OP_DEL); // The first chunk is a special case
    }
    dictOperator(chunks, chunk, chunkFirstTSAfterOp, DICT_OP_SET);
}

// 1.4版本引入的新功能，支持乱序插入
// TODO: 向上插入是如何实现的？？？
int SeriesUpsertSample(Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_override) {
    bool latestChunk = true;
    void *chunkKey = NULL;
    const ChunkFuncs *funcs = series->funcs;
    Chunk_t *chunk = series->lastChunk;
    timestamp_t chunkFirstTS = funcs->GetFirstTimestamp(series->lastChunk);

    // 如果当前的时间戳小于当前chunk的第一个时间戳并且当前时序key的chunk的数量大于1
    if (timestamp < chunkFirstTS && RedisModule_DictSize(series->chunks) > 1) {
        // Upsert in an older chunk
        // 向上插入一个较老的chunk
        latestChunk = false;
        timestamp_t rax_key;
        // 将timestamp转换为raxKey
        seriesEncodeTimestamp(&rax_key, timestamp);
        RedisModuleDictIter *dictIter =
            RedisModule_DictIteratorStartC(series->chunks, "<=", &rax_key, sizeof(rax_key));
        // 找到第一个大约等于这个时间戳的chunk
        chunkKey = RedisModule_DictNextC(dictIter, NULL, (void *)&chunk);
        if (chunkKey == NULL) {
            RedisModule_DictIteratorReseekC(dictIter, "^", NULL, 0);
            chunkKey = RedisModule_DictNextC(dictIter, NULL, (void *)&chunk);
        }
        RedisModule_DictIteratorStop(dictIter);
        if (chunkKey == NULL) {
            return REDISMODULE_ERR;
        }

        // 找到了要插入数据的chunk，并且获取到了对应chunk的一个时间戳
        chunkFirstTS = funcs->GetFirstTimestamp(chunk);
    }

    // Split chunks
    // 拆分chunks，如果chunk中数据的纯大小大于默认chunk的大小的1.2倍
    // TODO: 那么这里说明什么情况？？？之前扩展过？？？
    if (funcs->GetChunkSize(chunk, false) > series->chunkSizeBytes * SPLIT_FACTOR) {
        Chunk_t *newChunk = funcs->SplitChunk(chunk);
        if (newChunk == NULL) {
            return REDISMODULE_ERR;
        }

        // 将新chunk加入到chunk的dict（raxTree）中
        timestamp_t newChunkFirstTS = funcs->GetFirstTimestamp(newChunk);
        dictOperator(series->chunks, newChunk, newChunkFirstTS, DICT_OP_SET);

        // 如果要插入的时间戳比新的chunk的第一个时间戳大，这说明我们找都的chunk符合要求
        if (timestamp >= newChunkFirstTS) {
            chunk = newChunk;
            chunkFirstTS = newChunkFirstTS;
        }
        // 如果当前的chunk是最后一个chunk，那么整个时序key的最后一个chunk就是新创建的chunk
        if (latestChunk) { // split of latest chunk
            series->lastChunk = newChunk;
        }
    }

    // 解析来开始执行向上插入操作
    UpsertCtx uCtx = {
        .inChunk = chunk,
        .sample = { .timestamp = timestamp, .value = value },
    };

    int size = 0;

    // Use module level configuration if key level configuration doesn't exists
    // 如果密钥级配置不存在，则使用模块级配置
    DuplicatePolicy dp_policy;
    if (dp_override != DP_NONE) {
        dp_policy = dp_override;
    } else if (series->duplicatePolicy != DP_NONE) {
        dp_policy = series->duplicatePolicy;
    } else {
        dp_policy = TSGlobalConfig.duplicatePolicy;
    }

    // 针对于是否开启了压缩特性，内部提供了两种向上插入的方法
    ChunkResult rv = funcs->UpsertSample(&uCtx, &size, dp_policy);
    if (rv == CR_OK) {
        series->totalSamples += size;
        if (timestamp == series->lastTimestamp) {
            series->lastValue = uCtx.sample.value;
        }
        timestamp_t chunkFirstTSAfterOp = funcs->GetFirstTimestamp(uCtx.inChunk);
        if (chunkFirstTSAfterOp != chunkFirstTS) {
            update_chunk_in_dict(series->chunks, uCtx.inChunk, chunkFirstTS, chunkFirstTSAfterOp);
        }

        upsertCompaction(series, &uCtx);
    }
    return rv;
}

int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value) {
    // backfilling or update
    // 回填或者更新
    // 每一个数据样本就包含两部分，时间戳（longlong）和value（double类型）
    Sample sample = { .timestamp = timestamp, .value = value };
    ChunkResult ret = series->funcs->AddSample(series->lastChunk, &sample);

    // 如果 ret 等于 CR_END ，意味着 chunk 中已经没有空间来存储对应的时间戳和value了
    // 因此我们需要创建一个新的chunk
    if (ret == CR_END) {
        // When a new chunk is created trim the series
        // 当一个新的chunk被创建
        // 裁剪现有的chunk，chunk中可能有一个已经过期的key需要被删除
        SeriesTrim(series, 0, 0);

        // 创建新的chunk，并将其加入到chunks的dict中
        // 需要注意的是，在dict中key是时间戳，value是新创建的chunk
        // TODO: 每一个时间戳一个chunk 还是一个 chunk 中可能包含多个时间戳的数据？？？
        Chunk_t *newChunk = series->funcs->NewChunk(series->chunkSizeBytes);
        dictOperator(series->chunks, newChunk, timestamp, DICT_OP_SET);
        ret = series->funcs->AddSample(newChunk, &sample);
        series->lastChunk = newChunk;
    }
    series->lastTimestamp = timestamp;
    series->lastValue = value;
    series->totalSamples++;
    return TSDB_OK;
}

static int ContinuousDeletion(RedisModuleCtx *ctx,
                              Series *series,
                              CompactionRule *rule,
                              timestamp_t start,
                              timestamp_t end) {
    RedisModuleKey *key;
    Series *destSeries;
    if (!GetSeries(ctx,
                   rule->destKey,
                   &key,
                   &destSeries,
                   REDISMODULE_READ | REDISMODULE_WRITE,
                   false,
                   false)) {
        RedisModule_Log(ctx, "verbose", "%s", "Failed to retrieve downsample series");
        return TSDB_ERROR;
    }

    SeriesDelRange(destSeries, start, end);

    RedisModule_CloseKey(key);
    return TSDB_OK;
}

void CompactionDelRange(Series *series, timestamp_t start_ts, timestamp_t end_ts) {
    if (!series->rules)
        return;

    deleteReferenceToDeletedSeries(rts_staticCtx, series);
    CompactionRule *rule = series->rules;

    while (rule) {
        const timestamp_t ruleTimebucket = rule->bucketDuration;
        const timestamp_t curAggWindowStart =
            CalcBucketStart(series->lastTimestamp, ruleTimebucket, rule->timestampAlignment);
        const timestamp_t curAggWindowStartNormalized = BucketStartNormalize(curAggWindowStart);

        if (start_ts >= curAggWindowStartNormalized) {
            // All deletion range in latest timebucket - only update the context on the rule
            const int rv = SeriesCalcRange(series,
                                           curAggWindowStartNormalized,
                                           curAggWindowStart + ruleTimebucket - 1,
                                           rule,
                                           NULL,
                                           NULL);
            if (rv == TSDB_ERROR) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }
        } else {
            const timestamp_t startTSWindowStart =
                CalcBucketStart(start_ts, ruleTimebucket, rule->timestampAlignment);
            const timestamp_t startTSWindowStartNormalized =
                BucketStartNormalize(startTSWindowStart);
            const timestamp_t endTSWindowStart =
                CalcBucketStart(end_ts, ruleTimebucket, rule->timestampAlignment);
            const timestamp_t endTSWindowStartNormalized = BucketStartNormalize(endTSWindowStart);
            timestamp_t continuous_deletion_start;
            timestamp_t continuous_deletion_end;
            double val = 0;
            bool is_empty;
            int rv;

            // ---- handle start bucket ----

            rv = SeriesCalcRange(series,
                                 startTSWindowStartNormalized,
                                 startTSWindowStart + ruleTimebucket - 1,
                                 rule,
                                 &val,
                                 &is_empty);
            if (unlikely(rv == TSDB_ERROR)) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }

            if (is_empty) {
                // first bucket should be deleted
                continuous_deletion_start = startTSWindowStartNormalized;
            } else { // first bucket needs update
                // continuous deletion starts one bucket after startTSWindowStart
                continuous_deletion_start = startTSWindowStart + ruleTimebucket;
                if (!RuleSeriesUpsertSample(
                        rts_staticCtx, series, rule, startTSWindowStartNormalized, val)) {
                    continue;
                }
            }

            // ---- handle end bucket ----

            if (end_ts >= curAggWindowStartNormalized) {
                // deletion in latest timebucket
                const int rv = SeriesCalcRange(
                    series, curAggWindowStartNormalized, UINT64_MAX, rule, NULL, NULL);
                if (rv == TSDB_ERROR) {
                    RedisModule_Log(
                        rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                    continue;
                }
                // continuous deletion ends one bucket before endTSWindowStart
                continuous_deletion_end = BucketStartNormalize(endTSWindowStart - ruleTimebucket);
            } else {
                // deletion in old timebucket
                rv = SeriesCalcRange(series,
                                     endTSWindowStartNormalized,
                                     endTSWindowStart + ruleTimebucket - 1,
                                     rule,
                                     &val,
                                     &is_empty);
                if (unlikely(rv == TSDB_ERROR)) {
                    RedisModule_Log(
                        rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                    continue;
                }

                if (is_empty) {
                    // continuous deletion ends in end timebucket
                    continuous_deletion_end = endTSWindowStartNormalized;
                } else { // update in end timebucket
                    // continuous deletion ends one bucket before endTSWindowStart
                    continuous_deletion_end =
                        BucketStartNormalize(endTSWindowStart - ruleTimebucket);
                    if (!RuleSeriesUpsertSample(
                            rts_staticCtx, series, rule, endTSWindowStartNormalized, val)) {
                        continue;
                    }
                }
            }

            // ---- handle continuous deletion ----

            if (continuous_deletion_end >= continuous_deletion_start) {
                ContinuousDeletion(rts_staticCtx,
                                   series,
                                   rule,
                                   continuous_deletion_start,
                                   continuous_deletion_end);
            }
        }

        rule = rule->nextRule;
    }
}

// 范围删除样本数据
// TODO: 删除的逻辑还没有看
size_t SeriesDelRange(Series *series, timestamp_t start_ts, timestamp_t end_ts) {
    // start iterator from smallest key
    // 从最小的key开始迭代
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);

    Chunk_t *currentChunk;
    void *currentKey;
    size_t keyLen;
    size_t deletedSamples = 0;
    const ChunkFuncs *funcs = series->funcs;
    while ((currentKey = RedisModule_DictNextC(iter, &keyLen, (void *)&currentChunk))) {
        // We deleted the latest samples, no more chunks/samples to delete or cur chunk start_ts is
        // larger than end_ts
        if (!currentKey || (funcs->GetNumOfSample(currentChunk) == 0) ||
            funcs->GetFirstTimestamp(currentChunk) > end_ts) {
            // Having empty chunk means the series is empty
            break;
        }

        bool is_only_chunk =
            ((funcs->GetNumOfSample(currentChunk) + deletedSamples) == series->totalSamples);
        // Should we delete the all chunk?
        bool ts_delCondition =
            (funcs->GetFirstTimestamp(currentChunk) >= start_ts &&
             funcs->GetLastTimestamp(currentChunk) <= end_ts) &&
            (!is_only_chunk); // We assume at least one allocated chunk in the series

        if (!ts_delCondition) {
            timestamp_t chunkFirstTS = funcs->GetFirstTimestamp(currentChunk);
            deletedSamples += funcs->DelRange(currentChunk, start_ts, end_ts);
            timestamp_t chunkFirstTSAfterOp = funcs->GetFirstTimestamp(currentChunk);
            if (chunkFirstTSAfterOp != chunkFirstTS) {
                update_chunk_in_dict(
                    series->chunks, currentChunk, chunkFirstTS, chunkFirstTSAfterOp);
                // reseek iterator since we modified the dict,
                // go to first element that is bigger than current key
                timestamp_t rax_key;
                seriesEncodeTimestamp(&rax_key, chunkFirstTSAfterOp);
                RedisModule_DictIteratorReseekC(iter, ">", &rax_key, sizeof(rax_key));
            }
            continue;
        }

        bool isLastChunkDeleted = (currentChunk == series->lastChunk);
        RedisModule_DictDelC(series->chunks, currentKey, keyLen, NULL);
        deletedSamples += funcs->GetNumOfSample(currentChunk);
        funcs->FreeChunk(currentChunk);

        if (isLastChunkDeleted) {
            Chunk_t *lastChunk;
            RedisModuleDictIter *lastChunkIter =
                RedisModule_DictIteratorStartC(series->chunks, "$", NULL, 0);
            RedisModule_DictNextC(lastChunkIter, NULL, (void *)&lastChunk);
            series->lastChunk = lastChunk;
            RedisModule_DictIteratorStop(lastChunkIter);
        }

        // reseek iterator since we modified the dict,
        // go to first element that is bigger than current key
        RedisModule_DictIteratorReseekC(iter, ">", currentKey, keyLen);
    }
    series->totalSamples -= deletedSamples;

    RedisModule_DictIteratorStop(iter);

    // 删除聚合数据中指定范围的样本数据
    CompactionDelRange(series, start_ts, end_ts);

    // Check if last timestamp deleted
    if (end_ts >= series->lastTimestamp && start_ts <= series->lastTimestamp) {
        iter = RedisModule_DictIteratorStartC(series->chunks, "$", NULL, 0);
        currentKey = RedisModule_DictNextC(iter, &keyLen, (void *)&currentChunk);
        if (!currentKey || (funcs->GetNumOfSample(currentChunk) == 0)) {
            // No samples in the series
            series->lastTimestamp = 0;
            series->lastValue = 0;
        } else {
            series->lastTimestamp = funcs->GetLastTimestamp(currentChunk);
            series->lastValue = funcs->GetLastValue(currentChunk);
        }
        RedisModule_DictIteratorStop(iter);
    }
    return deletedSamples;
}

CompactionRule *SeriesAddRule(RedisModuleCtx *ctx,
                              Series *series,
                              Series *destSeries,
                              int aggType,
                              uint64_t bucketDuration,
                              timestamp_t timestampAlignment) {
    CompactionRule *rule =
        NewRule(destSeries->keyName, aggType, bucketDuration, timestampAlignment);
    if (rule == NULL) {
        return NULL;
    }
    RedisModule_RetainString(ctx, destSeries->keyName);
    if (series->rules == NULL) {
        series->rules = rule;
    } else {
        CompactionRule *last = series->rules;
        while (last->nextRule != NULL)
            last = last->nextRule;
        last->nextRule = rule;
    }
    return rule;
}

int SeriesCreateRulesFromGlobalConfig(RedisModuleCtx *ctx,
                                      RedisModuleString *keyName,
                                      Series *series,
                                      Label *labels,
                                      size_t labelsCount) {
    size_t len;
    int i;
    Series *compactedSeries;
    RedisModuleKey *compactedKey;
    size_t compactedRuleLabelCount = labelsCount + 2;

    for (i = 0; i < TSGlobalConfig.compactionRulesCount; i++) {
        SimpleCompactionRule *rule = TSGlobalConfig.compactionRules + i;
        const char *aggString = AggTypeEnumToString(rule->aggType);
        RedisModuleString *destKey;
        if (rule->timestampAlignment != 0) {
            destKey = RedisModule_CreateStringPrintf(ctx,
                                                     "%s_%s_%" PRIu64 "_%" PRIu64,
                                                     RedisModule_StringPtrLen(keyName, &len),
                                                     aggString,
                                                     rule->bucketDuration,
                                                     rule->timestampAlignment);
        } else {
            destKey = RedisModule_CreateStringPrintf(ctx,
                                                     "%s_%s_%" PRIu64,
                                                     RedisModule_StringPtrLen(keyName, &len),
                                                     aggString,
                                                     rule->bucketDuration);
        }

        compactedKey = RedisModule_OpenKey(ctx, destKey, REDISMODULE_READ | REDISMODULE_WRITE);
        if (RedisModule_KeyType(compactedKey) != REDISMODULE_KEYTYPE_EMPTY) {
            // TODO: should we break here? Is log enough?
            RM_LOG_WARNING(ctx,
                           "Cannot create compacted key, key '%s' already exists",
                           RedisModule_StringPtrLen(destKey, NULL));
            RedisModule_FreeString(ctx, destKey);
            RedisModule_CloseKey(compactedKey);
            continue;
        }

        Label *compactedLabels = calloc(compactedRuleLabelCount, sizeof(Label));
        // todo: deep copy labels function
        for (int l = 0; l < labelsCount; l++) {
            compactedLabels[l].key = RedisModule_CreateStringFromString(NULL, labels[l].key);
            compactedLabels[l].value = RedisModule_CreateStringFromString(NULL, labels[l].value);
        }

        // For every aggregated key create 2 labels: `aggregation` and `time_bucket`.
        compactedLabels[labelsCount].key = RedisModule_CreateStringPrintf(NULL, "aggregation");
        compactedLabels[labelsCount].value =
            RedisModule_CreateString(NULL, aggString, strlen(aggString));
        compactedLabels[labelsCount + 1].key = RedisModule_CreateStringPrintf(NULL, "time_bucket");
        compactedLabels[labelsCount + 1].value =
            RedisModule_CreateStringPrintf(NULL, "%" PRIu64, rule->bucketDuration);

        int rules_options = TSGlobalConfig.options;
        rules_options &= ~SERIES_OPT_DEFAULT_COMPRESSION;
        rules_options &= SERIES_OPT_UNCOMPRESSED;

        CreateCtx cCtx = {
            .retentionTime = rule->retentionSizeMillisec,
            .chunkSizeBytes = TSGlobalConfig.chunkSizeBytes,
            .labelsCount = compactedRuleLabelCount,
            .labels = compactedLabels,
            .options = rules_options,
        };
        CreateTsKey(ctx, destKey, &cCtx, &compactedSeries, &compactedKey);
        SeriesSetSrcRule(ctx, compactedSeries, series->keyName);
        SeriesAddRule(ctx,
                      series,
                      compactedSeries,
                      rule->aggType,
                      rule->bucketDuration,
                      rule->timestampAlignment);
        RedisModule_CloseKey(compactedKey);
    }
    return TSDB_OK;
}

CompactionRule *NewRule(RedisModuleString *destKey,
                        int aggType,
                        uint64_t bucketDuration,
                        uint64_t timestampAlignment) {
    if (bucketDuration == 0ULL) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)malloc(sizeof(CompactionRule));
    // 获取对应聚合类型的实现
    rule->aggClass = GetAggClass(aggType);
    rule->aggType = aggType;
    rule->aggContext = rule->aggClass->createContext(false);
    rule->bucketDuration = bucketDuration;
    rule->timestampAlignment = timestampAlignment;
    rule->destKey = destKey;
    rule->startCurrentTimeBucket = -1LL;
    rule->nextRule = NULL;

    return rule;
}

int SeriesDeleteRule(Series *series, RedisModuleString *destKey) {
    CompactionRule *rule = series->rules;
    CompactionRule *prev_rule = NULL;
    while (rule != NULL) {
        if (RMUtil_StringEquals(rule->destKey, destKey)) {
            CompactionRule *next = rule->nextRule;
            FreeCompactionRule(rule);
            if (prev_rule != NULL) {
                // cut off the current rule from the linked list
                prev_rule->nextRule = next;
            } else {
                // make the next one to be the first rule
                series->rules = next;
            }
            return TRUE;
        }
        prev_rule = rule;
        rule = rule->nextRule;
    }
    return FALSE;
}

void SeriesSetSrcRule(RedisModuleCtx *ctx, Series *series, RedisModuleString *srcKeyName) {
    RedisModule_RetainString(ctx, srcKeyName);
    series->srcKey = srcKeyName;
}

int SeriesDeleteSrcRule(Series *series, RedisModuleString *srctKey) {
    if (RMUtil_StringEquals(series->srcKey, srctKey)) {
        RedisModule_FreeString(NULL, series->srcKey);
        series->srcKey = NULL;
        return TRUE;
    }
    return FALSE;
}

/*
 * This function calculate aggregation value of a range.
 *
 * If `val` is NULL, the function will update the context of `rule`.
 */
int SeriesCalcRange(Series *series,
                    timestamp_t start_ts,
                    timestamp_t end_ts,
                    CompactionRule *rule,
                    double *val,
                    bool *is_empty) {
    Sample sample;
    AggregationClass *aggObject = rule->aggClass;
    void *context = aggObject->createContext(false);
    bool _is_empty = true;
    AbstractSampleIterator *iterator;
    RangeArgs args = { .aggregationArgs = { 0 },
                       .filterByValueArgs = { 0 },
                       .filterByTSArgs = { 0 } };

    if (aggObject->addBucketParams) {
        aggObject->addBucketParams(context, start_ts, end_ts + 1);
    }

    if (aggObject->addPrevBucketLastSample && start_ts > 0) {
        args.startTimestamp = 0, args.endTimestamp = start_ts - 1,
        iterator = SeriesCreateSampleIterator(series, &args, true, true);
        if (iterator->GetNext(iterator, &sample) == CR_OK) {
            aggObject->addPrevBucketLastSample(context, sample.value, sample.timestamp);
        }
        iterator->Close(iterator);
    }

    args.startTimestamp = start_ts;
    args.endTimestamp = end_ts;
    iterator = SeriesCreateSampleIterator(series, &args, false, true);

    while (iterator->GetNext(iterator, &sample) == CR_OK) {
        aggObject->appendValue(context, sample.value, sample.timestamp);
        _is_empty = false;
    }
    iterator->Close(iterator);

    if (aggObject->addNextBucketFirstSample) {
        args.startTimestamp = end_ts + 1, args.endTimestamp = UINT64_MAX,
        iterator = SeriesCreateSampleIterator(series, &args, false, true);
        if (iterator->GetNext(iterator, &sample) == CR_OK) {
            aggObject->addNextBucketFirstSample(context, sample.value, sample.timestamp);
        }
        iterator->Close(iterator);
    }

    if (is_empty) {
        *is_empty = _is_empty;
    }

    if (val == NULL) { // just update context for current window
        aggObject->freeContext(rule->aggContext);
        rule->aggContext = context;
    } else {
        if (!_is_empty) {
            aggObject->finalize(context, val);
        }
        aggObject->freeContext(context);
    }
    return TSDB_OK;
}

timestamp_t getFirstValidTimestamp(Series *series, long long *skipped) {
    if (skipped != NULL) {
        *skipped = 0;
    }
    if (series->totalSamples == 0) {
        return 0;
    }

    size_t count = 0;
    Sample sample = { 0 };

    timestamp_t minTimestamp = 0;
    if (series->retentionTime && series->retentionTime < series->lastTimestamp) {
        minTimestamp = series->lastTimestamp - series->retentionTime;
    }

    const RangeArgs args = { .startTimestamp = 0,
                             .endTimestamp = series->lastTimestamp,
                             .aggregationArgs = { 0 },
                             .filterByValueArgs = { 0 },
                             .filterByTSArgs = { 0 } };
    AbstractSampleIterator *iterator = SeriesCreateSampleIterator(series, &args, false, false);

    while (iterator->GetNext(iterator, &sample) == CR_OK) {
        if (sample.timestamp >= minTimestamp) {
            break;
        }
        ++count;
    }

    if (skipped != NULL) {
        *skipped = count;
    }
    iterator->Close(iterator);
    return sample.timestamp;
}

// 正序查询的时候，其中check_retention是true
AbstractIterator *SeriesQuery(Series *series,
                              const RangeArgs *args,
                              bool reverse,
                              bool check_retention) {
    // In case a retention is set shouldn't return chunks older than the retention
    // 如果设置了保留，则不应返回早于保留的块
    timestamp_t startTimestamp = args->startTimestamp;
    if (check_retention && series->retentionTime > 0) {
        // series->lastTimestamp - series->retentionTime 就是要保留的最老的时间数据
        // 第一种情况，要保留的时间数据在start和last范围内，那么start就挪到要保留的最老的时间点上
        // 第二种情况，要保留的时间数据超出了start和last的范围，但是由于当前最老的就是start，那就从start开始
        startTimestamp =
            series->lastTimestamp > series->retentionTime
                ? max(args->startTimestamp, series->lastTimestamp - series->retentionTime)
                : args->startTimestamp;
    }

    // When there is a TS filter because we wanted the logic to be one for both reverse and non
    // reverse chunk, if the requested range should be reverse, we reverse it after the filter, and
    // should_reverse_chunk point it out.
    // 当我们希望反向块和非反向块的逻辑都是一个TS过滤器时，
    // 如果请求的范围应该是反向的，我们在过滤器之后反转它，并且应该指出它。
    bool should_reverse_chunk = reverse && (!args->filterByTSArgs.hasValue);
    // 解析来要遍历所有的chunk了
    AbstractIterator *chain = SeriesIterator_New(
        series, startTimestamp, args->endTimestamp, reverse, should_reverse_chunk);

    if (args->filterByTSArgs.hasValue) {
        chain =
            (AbstractIterator *)SeriesFilterTSIterator_New(chain, args->filterByTSArgs, reverse);
    }

    if (args->filterByValueArgs.hasValue) {
        chain = (AbstractIterator *)SeriesFilterValIterator_New(chain, args->filterByValueArgs);
    }

    // 时间对齐方式
    // TODO: 时间对齐与传入的时间参数进行对齐的目的是？？？
    // TODO: 还会与传入的时间的参数进行对齐？？？
    timestamp_t timestampAlignment;
    switch (args->alignment) {
        case StartAlignment:
            // args-startTimestamp can hold an older timestamp than what we currently have or just 0
            //  args-startTimestamp 可以保存比当前时间戳旧的时间戳，或仅0
            timestampAlignment = args->startTimestamp;
            break;
        case EndAlignment:
            timestampAlignment = args->endTimestamp;
            break;
        case TimestampAlignment:
            timestampAlignment = args->timestampAlignment;
            break;
        default:
            timestampAlignment = 0; // TODO: 不进行时间对齐？？？
            break;
    }

    // 聚合类的数据
    if (args->aggregationArgs.aggregationClass != NULL) {
        chain = (AbstractIterator *)AggregationIterator_New(chain,
                                                            args->aggregationArgs.aggregationClass,
                                                            args->aggregationArgs.timeDelta,
                                                            timestampAlignment,
                                                            reverse,
                                                            args->aggregationArgs.empty,
                                                            args->aggregationArgs.bucketTS,
                                                            series);
    }

    return chain;
}

AbstractSampleIterator *SeriesCreateSampleIterator(Series *series,
                                                   const RangeArgs *args,
                                                   bool reverse,
                                                   bool check_retention) {
    AbstractIterator *chain = SeriesQuery(series, args, reverse, check_retention);
    return (AbstractSampleIterator *)SeriesSampleIterator_New(chain);
}

// returns sample iterator over multiple series
AbstractMultiSeriesSampleIterator *MultiSeriesCreateSampleIterator(Series **series,
                                                                   size_t n_series,
                                                                   const RangeArgs *args,
                                                                   bool reverse,
                                                                   bool check_retention) {
    size_t i;
    AbstractSampleIterator *iters[n_series];
    for (i = 0; i < n_series; ++i) {
        iters[i] = SeriesCreateSampleIterator(series[i], args, reverse, check_retention);
    }
    return (AbstractMultiSeriesSampleIterator *)MultiSeriesSampleIterator_New(
        iters, n_series, reverse);
}

// returns sample iterator over multiple series
AbstractSampleIterator *MultiSeriesCreateAggDupSampleIterator(Series **series,
                                                              size_t n_series,
                                                              const RangeArgs *args,
                                                              bool reverse,
                                                              bool check_retention,
                                                              const ReducerArgs *reducerArgs) {
    AbstractMultiSeriesSampleIterator *chain =
        MultiSeriesCreateSampleIterator(series, n_series, args, reverse, check_retention);
    return (AbstractSampleIterator *)MultiSeriesAggDupSampleIterator_New(chain, reducerArgs);
}
