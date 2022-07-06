/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef TSDB_H
#define TSDB_H

#include "abstract_iterator.h"
#include "compaction.h"
#include "consts.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "query_language.h"
#include "redismodule.h"

// 压缩规则
typedef struct CompactionRule
{
    RedisModuleString *destKey; // 目标（压缩）时间序列的键名，应该是一个时序类型，并且应该在ts.createrule之前创建
    timestamp_t bucketDuration; // 每个桶的间隔时间，以毫秒为单位
    timestamp_t timestampAlignment; // 对齐的时间戳，是一个时间点，后续每个对齐都以该时间点为起始位置点，毫秒为单位
    AggregationClass *aggClass;
    TS_AGG_TYPES_T aggType;
    void *aggContext;
    struct CompactionRule *nextRule;
    timestamp_t startCurrentTimeBucket; // Beware that the first bucket is alway starting in 0 no
                                        // matter the alignment
                                        // 请注意，无论对齐方式如何，第一个桶始终从0开始
                                        // 创建rule之后，该值的默认值为-1
} CompactionRule;

typedef struct Series
{
    RedisModuleDict *chunks;
    Chunk_t *lastChunk;
    uint64_t retentionTime; // TODO: 该值应该值得是一个时间范围，比如要保留60秒的数据，那么该值就是60
    long long chunkSizeBytes; // 每个chunk的字节大小
    short options;
    CompactionRule *rules; // 压缩规则
    timestamp_t lastTimestamp; // 上一次写入的时间戳
    double lastValue; // 上一次写入的value
    // labels是一块连续的内存，labels使用数组来进行管理
    Label *labels; // 标签，用于二级索引，在创建ts时指定 ，可以通过ts.alter命令来修改时序中的标签信息
    RedisModuleString *keyName;
    size_t labelsCount; // 标签的计数
    RedisModuleString *srcKey; // TODO: srcKey是什么意思？？？
    const ChunkFuncs *funcs;
    size_t totalSamples;
    DuplicatePolicy duplicatePolicy;
} Series;

// process C's modulo result to translate from a negative modulo to a positive
// 处理C的模结果，将负模转换为正模
#define modulo(x, N) ((x % N + N) % N)

// Calculate the begining of aggregation bucket
// 计算聚合bucket的开始点
static inline timestamp_t CalcBucketStart(timestamp_t ts,
                                          timestamp_t bucketDuration,
                                          timestamp_t timestampAlignment) {
    // ts为插入的时间戳，timestampAlignment为对齐的时间戳
    // TODO: 两者之差代表着什么？？？
    const int64_t timestamp_diff = ts - timestampAlignment;
    // modulo(timestamp_diff, (int64_t)bucketDuration) 转换为：
    // （timestamp_diff % bucketDuration + bucketDuration）% bucketDuration
    // 假设ts为100，bucketDuration为20，timestampAlignment为30，则最后的返回数据为：
    // （70 % 20 + 20） % 20 = 10
    // 70 - 10 = 80
    // TODO: 80这个时间戳代表着什么呢？？？
    return ts - modulo(timestamp_diff, (int64_t)bucketDuration);
}

// If bucketTS is negative converts it to 0
// 如果 bucketTS 是负数，则将其转换为0
static inline timestamp_t BucketStartNormalize(timestamp_t bucketTS) {
    // 如果按照上面假设的场景，bucketTS 为80，那么当前这个函数会返回80
    return max(0, (int64_t)bucketTS);
}

Series *NewSeries(RedisModuleString *keyName, CreateCtx *cCtx);
void FreeSeries(void *value);
void *CopySeries(RedisModuleString *fromkey, RedisModuleString *tokey, const void *value);
void RenameSeriesFrom(RedisModuleCtx *ctx, RedisModuleString *key);
void IndexMetricFromName(RedisModuleCtx *ctx, RedisModuleString *keyname);
void RenameSeriesTo(RedisModuleCtx *ctx, RedisModuleString *key);
void RestoreKey(RedisModuleCtx *ctx, RedisModuleString *keyname);

CompactionRule *GetRule(CompactionRule *rules, RedisModuleString *keyName);
void deleteReferenceToDeletedSeries(RedisModuleCtx *ctx, Series *series);

// Deletes the reference if the series deleted, watch out of rules iterator invalidation
int GetSeries(RedisModuleCtx *ctx,
              RedisModuleString *keyName,
              RedisModuleKey **key,
              Series **series,
              int mode,
              bool shouldDeleteRefs,
              bool isSilent);

AbstractIterator *SeriesQuery(Series *series,
                              const RangeArgs *args,
                              bool reserve,
                              bool check_retention);
AbstractSampleIterator *SeriesCreateSampleIterator(Series *series,
                                                   const RangeArgs *args,
                                                   bool reverse,
                                                   bool check_retention);

AbstractMultiSeriesSampleIterator *MultiSeriesCreateSampleIterator(Series **series,
                                                                   size_t n_series,
                                                                   const RangeArgs *args,
                                                                   bool reverse,
                                                                   bool check_retention);

AbstractSampleIterator *MultiSeriesCreateAggDupSampleIterator(Series **series,
                                                              size_t n_series,
                                                              const RangeArgs *args,
                                                              bool reverse,
                                                              bool check_retention,
                                                              const ReducerArgs *reducerArgs);

void FreeCompactionRule(void *value);
size_t SeriesMemUsage(const void *value);

int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value);
int SeriesUpsertSample(Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_override);

int SeriesDeleteRule(Series *series, RedisModuleString *destKey);
void SeriesSetSrcRule(RedisModuleCtx *ctx, Series *series, RedisModuleString *srcKeyName);
int SeriesDeleteSrcRule(Series *series, RedisModuleString *srctKey);

CompactionRule *SeriesAddRule(RedisModuleCtx *ctx,
                              Series *series,
                              Series *destSeries,
                              int aggType,
                              uint64_t bucketDuration,
                              timestamp_t timestampAlignment);
int SeriesCreateRulesFromGlobalConfig(RedisModuleCtx *ctx,
                                      RedisModuleString *keyName,
                                      Series *series,
                                      Label *labels,
                                      size_t labelsCount);
size_t SeriesGetNumSamples(const Series *series);

char *SeriesGetCStringLabelValue(const Series *series, const char *labelKey);
size_t SeriesDelRange(Series *series, timestamp_t start_ts, timestamp_t end_ts);
const char *SeriesChunkTypeToString(const Series *series);

int SeriesCalcRange(Series *series,
                    timestamp_t start_ts,
                    timestamp_t end_ts,
                    CompactionRule *rule,
                    double *val,
                    bool *is_empty);

// return first timestamp in retention window, and set `skipped` to number of samples outside of
// retention
timestamp_t getFirstValidTimestamp(Series *series, long long *skipped);

CompactionRule *NewRule(RedisModuleString *destKey,
                        int aggType,
                        uint64_t bucketDuration,
                        timestamp_t timestampAlignment);

// set/delete/replace a chunk in a dictionary
typedef enum
{
    DICT_OP_SET = 0,
    DICT_OP_REPLACE = 1,
    DICT_OP_DEL = 2
} DictOp;
int dictOperator(RedisModuleDict *d, void *chunk, timestamp_t ts, DictOp op);

void seriesEncodeTimestamp(void *buf, timestamp_t timestamp);

#endif /* TSDB_H */
