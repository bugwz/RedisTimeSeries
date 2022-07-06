/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#define REDISMODULE_MAIN

#include "module.h"

#include "LibMR/src/cluster.h"
#include "LibMR/src/mr.h"
#include "RedisModulesSDK/redismodule.h"
#include "common.h"
#include "compaction.h"
#include "config.h"
#include "fast_double_parser_c/fast_double_parser_c.h"
#include "indexer.h"
#include "libmr_commands.h"
#include "libmr_integration.h"
#include "query_language.h"
#include "rdb.h"
#include "reply.h"
#include "resultset.h"
#include "short_read.h"
#include "tsdb.h"
#include "version.h"

#include <ctype.h>
#include <limits.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include "rmutil/alloc.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"

#ifndef REDISTIMESERIES_GIT_SHA
#define REDISTIMESERIES_GIT_SHA "unknown"
#endif

RedisModuleType *SeriesType;
RedisModuleCtx *rts_staticCtx; // global redis ctx
bool isTrimming = false;

// 返回时间序列的信息和统计信息
// TS.INFO key [DEBUG]
// 如果带有debug参数将返回更多的信息
int TSDB_info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2 || argc > 3) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleKey *key;
    int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ, true, false);
    if (!status) {
        return REDISMODULE_ERR;
    }

    // 
    int is_debug = RMUtil_ArgExists("DEBUG", argv, argc, 1);
    if (is_debug) {
        RedisModule_ReplyWithArray(ctx, 14 * 2);
    } else {
        RedisModule_ReplyWithArray(ctx, 12 * 2);
    }

    long long skippedSamples;
    long long firstTimestamp = getFirstValidTimestamp(series, &skippedSamples);

    // 该时间序列中的样本总数
    RedisModule_ReplyWithSimpleString(ctx, "totalSamples");
    RedisModule_ReplyWithLongLong(ctx, SeriesGetNumSamples(series) - skippedSamples);
    // 这个时间序列分配的总字节数，包括结构体所占用的内存大小
    RedisModule_ReplyWithSimpleString(ctx, "memoryUsage");
    RedisModule_ReplyWithLongLong(ctx, SeriesMemUsage(series));
    // 时间序列中出现的第一个时间戳
    RedisModule_ReplyWithSimpleString(ctx, "firstTimestamp");
    RedisModule_ReplyWithLongLong(ctx, firstTimestamp);
    // 时间序列中存在的最后一个时间戳
    RedisModule_ReplyWithSimpleString(ctx, "lastTimestamp");
    RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
    // 时间序列的保留期，以毫秒为单位
    RedisModule_ReplyWithSimpleString(ctx, "retentionTime");
    RedisModule_ReplyWithLongLong(ctx, series->retentionTime);
    // 此时间序列的内存块数，TODO： 由于chunk的大小可被调整，因此有可能每个chunk的大小不一致？？？
    RedisModule_ReplyWithSimpleString(ctx, "chunkCount");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_DictSize(series->chunks));
    // 数据分配的内存大小，以字节为单位，TODO: 是否只能代表当前最新的大小？？？
    RedisModule_ReplyWithSimpleString(ctx, "chunkSize");
    RedisModule_ReplyWithLongLong(ctx, series->chunkSizeBytes);
    // 块类型：compressed或uncompressed，TODO: 是否压缩能否进行修改？？？
    RedisModule_ReplyWithSimpleString(ctx, "chunkType");
    RedisModule_ReplyWithSimpleString(ctx, ChunkTypeToString(series->options));
    // 这个时间序列的重复策略，可以通过ts.alter进行修改
    RedisModule_ReplyWithSimpleString(ctx, "duplicatePolicy");
    if (series->duplicatePolicy != DP_NONE) {
        RedisModule_ReplyWithSimpleString(ctx, DuplicatePolicyToString(series->duplicatePolicy));
    } else {
        RedisModule_ReplyWithNull(ctx);
    }

    // 此时间序列的元数据标签的标签值对的嵌套数组
    RedisModule_ReplyWithSimpleString(ctx, "labels");
    ReplyWithSeriesLabels(ctx, series);

    // 源时间序列的键名，以防当前序列是压缩规则的目标
    RedisModule_ReplyWithSimpleString(ctx, "sourceKey");
    if (series->srcKey == NULL) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        RedisModule_ReplyWithString(ctx, series->srcKey);
    }

    // 此时间序列中定义的压缩规则的嵌套数组
    RedisModule_ReplyWithSimpleString(ctx, "rules");
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    CompactionRule *rule = series->rules;
    int ruleCount = 0;
    while (rule != NULL) {
        RedisModule_ReplyWithArray(ctx, 4);
        // 压缩键
        RedisModule_ReplyWithString(ctx, rule->destKey);
        // 桶持续时间
        RedisModule_ReplyWithLongLong(ctx, rule->bucketDuration);
        // 聚合类型
        RedisModule_ReplyWithSimpleString(ctx, AggTypeEnumToString(rule->aggType));
        // 对齐时间戳
        RedisModule_ReplyWithLongLong(ctx, rule->timestampAlignment);

        rule = rule->nextRule;
        ruleCount++;
    }
    RedisModule_ReplySetArrayLength(ctx, ruleCount);

    // 指定时DEBUG，响应将包含一个名为 的附加数组字段Chunks。每个项目（每块）将包含
    if (is_debug) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, ">", "", 0);
        Chunk_t *chunk = NULL;
        int chunkCount = 0;
        // 对应时序key的名称
        RedisModule_ReplyWithSimpleString(ctx, "keySelfName");
        RedisModule_ReplyWithString(ctx, series->keyName);
        // chunks的详细信息
        RedisModule_ReplyWithSimpleString(ctx, "Chunks");
        RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
        while (RedisModule_DictNextC(iter, NULL, (void *)&chunk)) {
            u_int64_t numOfSamples = series->funcs->GetNumOfSample(chunk);
            size_t chunkSize = series->funcs->GetChunkSize(chunk, FALSE);
            RedisModule_ReplyWithArray(ctx, 5 * 2);
            // 块中存在的第一个时间戳
            RedisModule_ReplyWithSimpleString(ctx, "startTimestamp");
            RedisModule_ReplyWithLongLong(
                ctx, numOfSamples == 0 ? -1 : series->funcs->GetFirstTimestamp(chunk));
            // 块中存在的最后一个时间戳
            RedisModule_ReplyWithSimpleString(ctx, "endTimestamp");
            RedisModule_ReplyWithLongLong(
                ctx, numOfSamples == 0 ? -1 : series->funcs->GetLastTimestamp(chunk));
            // 块中的样本总数
            RedisModule_ReplyWithSimpleString(ctx, "samples");
            RedisModule_ReplyWithLongLong(ctx, numOfSamples);
            // 以字节为单位的块数据大小（这是仅用于块内数据的确切大小，不包括其他开销）
            RedisModule_ReplyWithSimpleString(ctx, "size");
            RedisModule_ReplyWithLongLong(ctx, chunkSize);
            // 比率size和samples
            RedisModule_ReplyWithSimpleString(ctx, "bytesPerSample");
            RedisModule_ReplyWithDouble(ctx, (float)chunkSize / numOfSamples);
            chunkCount++;
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_ReplySetArrayLength(ctx, chunkCount);
    }
    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

// 查询二级索引
void _TSDB_queryindex_impl(RedisModuleCtx *ctx, QueryPredicateList *queries) {
    // 查询出的二级索引是一个dict，这个dict是一个rax结构
    RedisModuleDict *result = QueryIndex(ctx, queries->list, queries->count);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    // 遍历dict，获取符合条件的索引
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModule_ReplyWithStringBuffer(ctx, currentKey, currentKeyLen);
        replylen++;
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplySetArrayLength(ctx, replylen);
}

// 获取与过滤器列表匹配的所有时间序列键
// 在 Redis 集群上运行时，QUERYINDEX 命令不能作为事务的一部分。
// TS.QUERYINDEX filter...
int TSDB_queryindex(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    int query_count = argc - 1;

    int response = 0;
    QueryPredicateList *queries = parseLabelListFromArgs(ctx, argv, 1, query_count, &response);
    if (response == TSDB_ERROR) {
        QueryPredicateList_Free(queries);
        return RTS_ReplyGeneralError(ctx, "TSDB: failed parsing labels");
    }

    // 无论何时需要提供过滤器，都必须应用至少一个标签=值过滤器
    if (CountPredicateType(queries, EQ) + CountPredicateType(queries, LIST_MATCH) == 0) {
        QueryPredicateList_Free(queries);
        return RTS_ReplyGeneralError(ctx, "TSDB: please provide at least one matcher");
    }

    if (IsMRCluster()) {
        int ctxFlags = RedisModule_GetContextFlags(ctx);

        if (ctxFlags & (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI |
                        REDISMODULE_CTX_FLAGS_DENY_BLOCKING)) {
            RedisModule_ReplyWithError(ctx,
                                       "Can not run multi sharded command inside a multi exec, "
                                       "lua, or when blocking is not allowed");
            return REDISMODULE_OK;
        }
        TSDB_queryindex_RG(ctx, queries);
        QueryPredicateList_Free(queries);
    } else {
        _TSDB_queryindex_impl(ctx, queries);
        QueryPredicateList_Free(queries);
    }

    return REDISMODULE_OK;
}

// multi-series groupby logic
static int replyGroupedMultiRange(RedisModuleCtx *ctx,
                                  TS_ResultSet *resultset,
                                  RedisModuleDict *result,
                                  const MRangeArgs *args) {
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey = NULL;
    size_t currentKeyLen;
    Series *series = NULL;

    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        const int status = GetSeries(ctx,
                                     RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                                     &key,
                                     &series,
                                     REDISMODULE_READ,
                                     false,
                                     true);
        if (!status) {
            RedisModule_Log(
                ctx, "warning", "couldn't open key or key is not a Timeseries. key=%s", currentKey);
            // The iterator may have been invalidated, stop and restart from after the current
            // key.
            RedisModule_DictIteratorStop(iter);
            iter = RedisModule_DictIteratorStartC(result, ">", currentKey, currentKeyLen);
            continue;
        }
        ResultSet_AddSerie(resultset, series, RedisModule_StringPtrLen(series->keyName, NULL));
        RedisModule_CloseKey(key);
    }
    RedisModule_DictIteratorStop(iter);

    // todo: this is duplicated in resultset.c
    // Apply the reducer
    ResultSet_ApplyReducer(resultset, &args->rangeArgs, &args->gropuByReducerArgs);

    // Do not apply the aggregation on the resultset, do apply max results on the final result
    RangeArgs minimizedArgs = args->rangeArgs;
    minimizedArgs.startTimestamp = 0;
    minimizedArgs.endTimestamp = UINT64_MAX;
    minimizedArgs.aggregationArgs.aggregationClass = NULL;
    minimizedArgs.aggregationArgs.timeDelta = 0;
    minimizedArgs.filterByTSArgs.hasValue = false;
    minimizedArgs.filterByValueArgs.hasValue = false;

    replyResultSet(ctx,
                   resultset,
                   args->withLabels,
                   (RedisModuleString **)args->limitLabels,
                   args->numLimitLabels,
                   &minimizedArgs,
                   args->reverse);

    ResultSet_Free(resultset);
    return REDISMODULE_OK;
}

// Previous multirange reply logic ( unchanged )
static int replyUngroupedMultiRange(RedisModuleCtx *ctx,
                                    RedisModuleDict *result,
                                    const MRangeArgs *args) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    Series *series;
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        const int status = GetSeries(ctx,
                                     RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                                     &key,
                                     &series,
                                     REDISMODULE_READ,
                                     false,
                                     true);

        if (!status) {
            RedisModule_Log(ctx,
                            "warning",
                            "couldn't open key or key is not a Timeseries. key=%.*s",
                            (int)currentKeyLen,
                            currentKey);
            // The iterator may have been invalidated, stop and restart from after the current key.
            RedisModule_DictIteratorStop(iter);
            iter = RedisModule_DictIteratorStartC(result, ">", currentKey, currentKeyLen);
            continue;
        }
        ReplySeriesArrayPos(ctx,
                            series,
                            args->withLabels,
                            (RedisModuleString **)args->limitLabels,
                            args->numLimitLabels,
                            &args->rangeArgs,
                            args->reverse);
        replylen++;
        RedisModule_CloseKey(key);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplySetArrayLength(ctx, replylen);

    return REDISMODULE_OK;
}

int TSDB_generic_mrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool rev) {
    RedisModule_AutoMemory(ctx);

    MRangeArgs args;
    if (parseMRangeCommand(ctx, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }
    args.reverse = rev;

    RedisModuleDict *resultSeries =
        QueryIndex(ctx, args.queryPredicates->list, args.queryPredicates->count);

    int result = REDISMODULE_OK;
    if (args.groupByLabel) {
        TS_ResultSet *resultset = ResultSet_Create();
        ResultSet_GroupbyLabel(resultset, args.groupByLabel);

        result = replyGroupedMultiRange(ctx, resultset, resultSeries, &args);
    } else {
        result = replyUngroupedMultiRange(ctx, resultSeries, &args);
    }

    MRangeArgs_Free(&args);
    return result;
}

// 通过正向过滤器查询跨多个时间序列的范围
// TODO: 过于复杂，需要详细去看
// TS.MRANGE
//          fromTimestamp : 范围查询的开始时间戳。用于-表示可能的最小时间戳
//          toTimestamp : 范围查询的结束时间戳。用于+表示最大可能的时间戳
//          [LATEST] : 暂未支持该配置
//          [FILTER_BY_TS ts...] ：后跟时间戳列表按特定时间戳过滤结果
//          [FILTER_BY_VALUE min max] ： 传入的最小值最大值，后续对结果进行过滤
//          [WITHLABELS | SELECTED_LABELS label...] ： 
//               指定WITHLABELS ： 回复中包含表示时间序列元数据标签的所有标签值对
//               指定SELECTED_LABELS label... ： 返回代表时间序列元数据标签的标签值对的子集
//               未指定：默认情况下，空列表将报告为标签值对
//          [COUNT count] ： 限制返回样本的数量
//          [
//             [ALIGN value] ：是AGGREGATION. 它通过更改定义存储桶的参考时间戳来控制时间存储桶时间戳
//                 startor： 参考时间戳将是查询开始间隔时间，不能是-
//                 endor： 参考时间戳将是查询结束间隔时间，不能是+
//                 特定时间戳：将参考时间戳与特定时间对齐
//             AGGREGATION aggregator bucketDuration ：将结果聚合到时间桶中，bucketDuration是每个桶的持续时间，以毫秒为单位
//             [BUCKETTIMESTAMP bt] ：控制桶时间戳的报告方式
//                 -或者low：时间戳是开始时间（默认）
//                 +或者high：时间戳时结束时间
//                 ~或者mid：时间戳是中间时间（如果不是整数，则向下舍入）
//             [EMPTY] ：是一个标志，当指定时，它会报告空桶的聚合
//          ] ： 它通过更改定义存储桶的参考时间戳来控制时间存储桶时间戳
//          FILTER filter.. ：使用过滤器
//          [GROUPBY label REDUCE reducer] ： 聚合不同时间序列的结果，按提供的标签名称分组，当与AGGREGATIONgroupby/reduce 结合使用后聚合阶段
//                   label：是用于对系列进行分组的标签名称。为每个值生成一个新系列
//                   reducer：是减速器类型，用于聚合共享相同标签值的系列
//                   
//   返回值：
//      键名
//      标签值对列表
//           默认情况下，报告一个空列表
//           如果WITHLABELS指定，则报告与此时间序列关联的所有标签
//           如果SELECTED_LABELS label...指定，则报告选定的标签
//      与范围匹配的所有样本/聚合的时间戳值对
// 
int TSDB_mrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // 当处于集群模式下的时候需要单独处理
    if (IsMRCluster()) {
        int ctxFlags = RedisModule_GetContextFlags(ctx);

        if (ctxFlags & (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI |
                        REDISMODULE_CTX_FLAGS_DENY_BLOCKING)) {
            RedisModule_ReplyWithError(ctx,
                                       "Can not run multi sharded command inside a multi exec, "
                                       "lua, or when blocking is not allowed");
            return REDISMODULE_OK;
        }

        // 如果是集群模式，走该逻辑
        return TSDB_mrange_RG(ctx, argv, argc, false);
    }

    // 非集群模式下
    return TSDB_generic_mrange(ctx, argv, argc, false);
}

int TSDB_mrevrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (IsMRCluster()) {
        int ctxFlags = RedisModule_GetContextFlags(ctx);

        if (ctxFlags & (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI |
                        REDISMODULE_CTX_FLAGS_DENY_BLOCKING)) {
            RedisModule_ReplyWithError(ctx,
                                       "Can not run multi sharded command inside a multi exec, "
                                       "lua, or when blocking is not allowed");
            return REDISMODULE_OK;
        }
        return TSDB_mrange_RG(ctx, argv, argc, true);
    }
    return TSDB_generic_mrange(ctx, argv, argc, true);
}

// 是否逆序的标记 rev 为false
int TSDB_generic_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool rev) {
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    // 获取源key的信息
    Series *series;
    RedisModuleKey *key;
    const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ, false, false);
    if (!status) {
        return REDISMODULE_ERR;
    }

    // 解析相关的参数
    RangeArgs rangeArgs = { 0 };
    if (parseRangeArguments(ctx, 2, argv, argc, series->lastTimestamp, &rangeArgs) !=
        REDISMODULE_OK) {
        goto _out;
    }

    ReplySeriesRange(ctx, series, &rangeArgs, rev);

_out:
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

// TODO: 最复杂的命令之一，需要详细研究
// ts.range命令
// 向前查询一个范围
// TS.RANGE
//         key : 时间序列的键名
//         fromTimestamp ：范围查询的开始时间戳。-可用于表示可能的最小时间戳
//         toTimestamp ：范围查询的结束时间戳，+可用于表示最大可能的时间戳
//         [LATEST] ： 暂未支持
//         [FILTER_BY_TS ts...] ： 后跟时间戳列表，以按特定时间戳过滤结果
//         [FILTER_BY_VALUE min max] ： 使用最小值和最大值按值过滤结果
//         [COUNT count]  ：返回样本的最大数量
//         [
//             [ALIGN value] ：聚合的时间桶对齐控制。这将通过更改定义存储桶的参考时间戳来控制时间存储桶时间戳
//                 可能的值：（注意：未提供时，对齐设置为0）
//                     startor ：参考时间戳将是查询开始间隔时间，不能是-
//                     endor：参考时间戳将是查询结束间隔时间，不能是+
//                     特定时间戳：将参考时间戳与特定时间对齐
//             AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]
//                  bucketDuration：聚合器 存储桶持续时间
//         ]

int TSDB_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return TSDB_generic_range(ctx, argv, argc, false);
}

int TSDB_revrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return TSDB_generic_range(ctx, argv, argc, true);
}

static void handleCompaction(RedisModuleCtx *ctx,
                             Series *series,
                             CompactionRule *rule,
                             api_timestamp_t timestamp,
                             double value) {

    // TODO: currentTimestamp 这个时间戳代表着什么呢？？？
    timestamp_t currentTimestamp =
        CalcBucketStart(timestamp, rule->bucketDuration, rule->timestampAlignment);
    // 按照 CalcBucketStart 函数内部的假设情况分析，currentTimestampNormalized 为 80
    timestamp_t currentTimestampNormalized = BucketStartNormalize(currentTimestamp);

    // 如果该值为-1，代表着创建rule之后还没有执行插入数据的操作
    if (rule->startCurrentTimeBucket == -1LL) {
        // first sample, lets init the startCurrentTimeBucket
        // 第一个示例，让我们初始化startCurrentTimeBucket
        // 第一次插入操作，在上面假设的情况下，也就是说将80赋值给它
        rule->startCurrentTimeBucket = currentTimestampNormalized;

        // 只有使用twa（时间加权平均值）的情况下 addBucketParams 才不为NULL
        if (rule->aggClass->addBucketParams) {
            rule->aggClass->addBucketParams(rule->aggContext,
                                            currentTimestampNormalized,
                                            currentTimestamp + rule->bucketDuration);
        }
    }

    // 在后续处理插入的情况下，只有当插入的时间序列较大时，才会执行下面逻辑
    // 实际上只有当新插入的时间与上次时间的差，超过了rule->bucketDuration才会进入到下面的逻辑
    if (currentTimestampNormalized > rule->startCurrentTimeBucket) {
        Series *destSeries;
        RedisModuleKey *key;
        // 获取对应的压缩key，压缩key中存储了对应的聚合数据
        int status = GetSeries(ctx,
                               rule->destKey,
                               &key,
                               &destSeries,
                               REDISMODULE_READ | REDISMODULE_WRITE,
                               false,
                               true);
        if (!status) {
            // key doesn't exist anymore and we don't do anything
            // 目标key不存在的时候什么都不能做
            return;
        }

        // 只有是权重平均数的时候才会执行到这里
        if (rule->aggClass->addNextBucketFirstSample) {
            rule->aggClass->addNextBucketFirstSample(rule->aggContext, value, timestamp);
        }

        // 获取聚合chunk中的val数据
        double aggVal;
        rule->aggClass->finalize(rule->aggContext, &aggVal);
        // 并将其加入的对应时间戳的val中
        SeriesAddSample(destSeries, rule->startCurrentTimeBucket, aggVal);
        // TODO: ts.add:dest的执行逻辑是？？？
        RedisModule_NotifyKeyspaceEvent(
            ctx, REDISMODULE_NOTIFY_MODULE, "ts.add:dest", rule->destKey);
        Sample last_sample;

        // 只有时间权重平均数的聚合规则才会走到这里
        if (rule->aggClass->addPrevBucketLastSample) {
            rule->aggClass->getLastSample(rule->aggContext, &last_sample);
        }

        // 需要重置聚合数据，以便于统计下一次的聚合数据
        rule->aggClass->resetContext(rule->aggContext);

        // 只有时间加权平均数的时候才会执行下面的逻辑，其他的聚合逻辑没有对应的执行函数
        if (rule->aggClass->addBucketParams) {
            rule->aggClass->addBucketParams(rule->aggContext,
                                            currentTimestampNormalized,
                                            currentTimestamp + rule->bucketDuration);
        }

        // 只有时间加权平均数的时候才会执行下面的逻辑，其他的聚合逻辑没有对应的执行函数
        if (rule->aggClass->addPrevBucketLastSample) {
            rule->aggClass->addPrevBucketLastSample(
                rule->aggContext, last_sample.value, last_sample.timestamp);
        }

        // 需要更新当前的时间戳，以便进入下一次的聚合统计
        rule->startCurrentTimeBucket = currentTimestampNormalized;
        RedisModule_CloseKey(key);
    }

    // 再执行聚合数据之前，我们需要记录一下按照对应聚合规则的数据的结果
    // 以便于在聚合操作的操作的时候直接获取对应聚合值
    rule->aggClass->appendValue(rule->aggContext, value, timestamp);
}

static int internalAdd(RedisModuleCtx *ctx,
                       Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_override) {
    timestamp_t lastTS = series->lastTimestamp;
    uint64_t retention = series->retentionTime;
    // ensure inside retention period.
    // TODO: 确保在保留期内
    // 保留时间约束，上次更新时间约束
    if (retention && timestamp < lastTS && retention < lastTS - timestamp) {
        RTS_ReplyGeneralError(ctx, "TSDB: Timestamp is older than retention");
        return REDISMODULE_ERR;
    }

    // 如果是乱序的样本，新样本的时间戳小于等于上次的时间戳，并且？？？
    if (timestamp <= series->lastTimestamp && series->totalSamples != 0) {
        // 乱序的样本在block模式下会发生错误
        // 时间戳较小，这里需要执行向上插入
        if (SeriesUpsertSample(series, timestamp, value, dp_override) != REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx,
                                  "TSDB: Error at upsert, update is not supported when "
                                  "DUPLICATE_POLICY is set to BLOCK mode");
            return REDISMODULE_ERR;
        }
    } else {
        // 将对应的时间戳和value添加到时序数据中
        if (SeriesAddSample(series, timestamp, value) != REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx, "TSDB: Error at add");
            return REDISMODULE_ERR;
        }
        // handle compaction rules
        // 处理压缩规则，删除没有引用的规则
        if (series->rules) {
            deleteReferenceToDeletedSeries(ctx, series);
        }

        // 遍历所有的规则，有可能需要针对不同的规则做一些处理，压缩规则
        // TODO: 聚合的数据会过期吗？？？
        CompactionRule *rule = series->rules;
        while (rule != NULL) {
            handleCompaction(ctx, series, rule, timestamp, value);
            rule = rule->nextRule;
        }
    }
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    return REDISMODULE_OK;
}

static inline int add(RedisModuleCtx *ctx,
                      RedisModuleString *keyName,
                      RedisModuleString *timestampStr,
                      RedisModuleString *valueStr,
                      RedisModuleString **argv,
                      int argc) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
    double value;
    const char *valueCStr = RedisModule_StringPtrLen(valueStr, NULL);
    if ((fast_double_parser_c_parse_number(valueCStr, &value) == NULL)) {
        RTS_ReplyGeneralError(ctx, "TSDB: invalid value");
        return REDISMODULE_ERR;
    }

    long long timestampValue;
    if ((RedisModule_StringToLongLong(timestampStr, &timestampValue) != REDISMODULE_OK)) {
        // if timestamp is "*", take current time (automatic timestamp)
        // 如果没有指定时间戳则使用系统的当前时间戳
        if (RMUtil_StringEqualsC(timestampStr, "*")) {
            timestampValue = RedisModule_Milliseconds();
        } else {
            RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp");
            return REDISMODULE_ERR;
        }
    }

    if (timestampValue < 0) {
        RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp, must be a nonnegative integer");
        return REDISMODULE_ERR;
    }
    api_timestamp_t timestamp = (u_int64_t)timestampValue;

    Series *series = NULL;
    DuplicatePolicy dp = DP_NONE;

    if (argv != NULL && RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        CreateCtx cCtx = { 0 };
        if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, &cCtx, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, cCtx.labels, cCtx.labelsCount);
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        RTS_ReplyGeneralError(ctx, "TSDB: the key is not a TSDB key");
        return REDISMODULE_ERR;
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
        //  overwride key and database configuration for DUPLICATE_POLICY
        // 重写key的策略：
        // block的时候只要乱序就出现问题
        // first忽略任务报告的新值
        // last用新报告的值覆盖旧值
        // min仅当值低于现有值时才覆盖
        // max仅当值高于现有值时才覆盖
        // sum如果存在先前的样本，则将新样本添加到其中，以使更新的值等于（先前的 + 新的）
        if (argv != NULL &&
            ParseDuplicatePolicy(ctx, argv, argc, TS_ADD_DUPLICATE_POLICY_ARG, &dp) != TSDB_OK) {
            return REDISMODULE_ERR;
        }
    }
    int rv = internalAdd(ctx, series, timestamp, value, dp);
    RedisModule_CloseKey(key);
    return rv;
}

// 将新样本附加到一个或多个时间序列，支持操作不同的key
// TS.MADD {key timestamp value}...
int TSDB_madd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4 || (argc - 1) % 3 != 0) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_ReplyWithArray(ctx, (argc - 1) / 3);
    RedisModuleString **replication_data = malloc(sizeof(RedisModuleString *) * (argc - 1));
    size_t replication_count = 0;
    for (int i = 1; i < argc; i += 3) {
        RedisModuleString *keyName = argv[i];
        RedisModuleString *timestampStr = argv[i + 1];
        RedisModuleString *valueStr = argv[i + 2];
        if (add(ctx, keyName, timestampStr, valueStr, NULL, -1) == REDISMODULE_OK) {
            replication_data[replication_count] = keyName;
            replication_data[replication_count + 1] = timestampStr;
            replication_data[replication_count + 2] = valueStr;
            replication_count += 3;
        }
    }

    if (replication_count > 0) {
        // we want to replicate only successful sample inserts to avoid errors on the replica, when
        // this errors occurs, redis will CRITICAL error to its log and potentially fill up the disk
        // depending on the actual traffic.
        RedisModule_Replicate(ctx, "TS.MADD", "v", replication_data, replication_count);
    }
    free(replication_data);

    for (int i = 1; i < argc; i += 3) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.add", argv[i]);
    }

    return REDISMODULE_OK;
}

// TS.ADD
//        key : 对应时序的key【必须参数】
//        timestamp ： 新加入样本时间戳【必须参数】
//        value ： 新加入样本的double类型数值，双精度遵循rfc7159标准【必须参数】
//        [RETENTION retentionPeriod] ： 最大保留时间，毫秒为单位，仅在创建新的时序key的时候使用，
//                                       默认为0（一直保留）【可选参数】
//        [ENCODING [COMPRESSED|UNCOMPRESSED]] ：存储样本的时候是否使用压缩，使用压缩能极大的减少内存占用，
//                                               仅在创建新的时序key的时候使用【可选参数】
//        [CHUNK_SIZE size] ：单个chunk的大小，一个chunk中可存储多个样本，仅在创建新的时序key的时候使用
//                            默认4k【可选参数】
//        [ON_DUPLICATE policy] ：处理具有相同时间戳样本的策略 【可选参数】
//                                  可选的参数：
//                                      BLOCK- 任何乱序样品都会发生错误
//                                      FIRST- 忽略任何新报告的值
//                                      LAST- 用新报告的值覆盖
//                                      MIN- 仅当值低于现有值时才覆盖
//                                      MAX- 仅当值高于现有值时才覆盖
//                                      SUM- 如果存在先前的样本，则将新样本添加到其中，以使更新的值等于（先前的 + 新的）。
//                                           如果不存在先前的样本，则将更新值设置为等于新值。
//                                  配置相关的维度：
//                                      全局维度：默认为block，只有当命令操作时未指定并且key维度
//                                              也没有指定该策略时才会使用全局维度的配置
//                                      key的维度：不指定则为none，只有当命令操作时未指定时才会使用当前维度的配置
//                                      命令操作维度（ts.add） ：不指定则为none
//        [LABELS {label value}...] ： 需要创建的二级索引，仅在创建新的时序key的时候使用 【可选参数】
int TSDB_add(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    RedisModuleString *timestampStr = argv[2];
    RedisModuleString *valueStr = argv[3];

    int result = add(ctx, keyName, timestampStr, valueStr, argv, argc);
    if (result == REDISMODULE_OK) {
        RedisModule_ReplicateVerbatim(ctx);
    }

    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.add", keyName);

    return result;
}

int CreateTsKey(RedisModuleCtx *ctx,
                RedisModuleString *keyName,
                CreateCtx *cCtx,
                Series **series,
                RedisModuleKey **key) {
    if (*key == NULL) {
        *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
    }

    RedisModule_RetainString(ctx, keyName);
    *series = NewSeries(keyName, cCtx);
    if (RedisModule_ModuleTypeSetValue(*key, SeriesType, *series) == REDISMODULE_ERR) {
        return TSDB_ERROR;
    }

    // 索引key的信息，创建时序key的时候就会创建
    IndexMetric(keyName, (*series)->labels, (*series)->labelsCount);

    return TSDB_OK;
}

// TS.CREATE 
//          key : 对应时序key的名字【必须参数】
//          [RETENTION retentionPeriod] ：设置要保留样本的时间戳，未指定的话默认为0【可选参数】
//          [ENCODING [UNCOMPRESSED|COMPRESSED]] ： 是否使用压缩编码，默认使用压缩编码【可选参数】 
//          [CHUNK_SIZE size] ：chunk的大小，单位是字节，默认为4K【可选参数】 
//          [DUPLICATE_POLICY policy] ： 遇到重复时间戳样本的策略 
//          [LABELS {label value}...] ：依据对应的label建立二级索引，这个索引信息并不持久化
int TSDB_create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleString *keyName = argv[1];
    CreateCtx cCtx = { 0 };
    if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RTS_ReplyGeneralError(ctx, "TSDB: key already exists");
    }

    CreateTsKey(ctx, keyName, &cCtx, &series, &key);
    RedisModule_CloseKey(key);

    RedisModule_Log(ctx, "verbose", "created new series");
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);

    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.create", keyName);

    return REDISMODULE_OK;
}

// TS.ALTER
//         key ：对应的时序key【必须参数】
//         [RETENTION retentionPeriod] ： 修改最大保留时间，单位毫秒【可选参数】
//         [CHUNK_SIZE size] ： 修改chunk的大小，不影响已有的chunk，只会影响新的chunk，单位字节【可选参数】 
//         [DUPLICATE_POLICY policy] ：处理具有相同时间戳的多个样本的策略【可选参数】 
//         [LABELS [{label value}...]] ： 变更二级索引信息，
//                                        如果LABELS指定，则应用给定的标签列表。
//                                        给定列表中不存在的标签将被隐式删除。
//                                        不指定LABELS标签值对将删除所有现有标签
int TSDB_alter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleKey *key;
    RedisModuleString *keyName = argv[1];
    CreateCtx cCtx = { 0 };
    if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    const int status =
        GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ | REDISMODULE_WRITE, false, false);
    if (!status) {
        return REDISMODULE_ERR;
    }
    if (RMUtil_ArgIndex("RETENTION", argv, argc) > 0) {
        series->retentionTime = cCtx.retentionTime;
    }

    if (RMUtil_ArgIndex("CHUNK_SIZE", argv, argc) > 0) {
        series->chunkSizeBytes = cCtx.chunkSizeBytes;
    }

    if (RMUtil_ArgIndex("DUPLICATE_POLICY", argv, argc) > 0) {
        series->duplicatePolicy = cCtx.duplicatePolicy;
    }

    if (RMUtil_ArgIndex("LABELS", argv, argc) > 0) {
        RemoveIndexedMetric(keyName);
        // free current labels
        FreeLabels(series->labels, series->labelsCount);

        // set new newLabels
        series->labels = cCtx.labels;
        series->labelsCount = cCtx.labelsCount;
        IndexMetric(keyName, series->labels, series->labelsCount);
    }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(key);

    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.alter", keyName);

    return REDISMODULE_OK;
}

/*
TS.DELETERULE SOURCE_KEY DEST_KEY
 */
int TSDB_deleteRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *srcKeyName = argv[1];

    // First try to remove the rule from the source key
    Series *srcSeries;
    RedisModuleKey *srcKey;
    const int statusS = GetSeries(
        ctx, srcKeyName, &srcKey, &srcSeries, REDISMODULE_READ | REDISMODULE_WRITE, true, false);
    if (!statusS) {
        return REDISMODULE_ERR;
    }

    RedisModuleString *destKeyName = argv[2];
    if (!SeriesDeleteRule(srcSeries, destKeyName)) {
        RedisModule_CloseKey(srcKey);
        return RTS_ReplyGeneralError(ctx, "TSDB: compaction rule does not exist");
    }

    // If succeed to remove the rule from the source key remove from the destination too
    Series *destSeries;
    RedisModuleKey *destKey;
    const int statusD = GetSeries(
        ctx, destKeyName, &destKey, &destSeries, REDISMODULE_READ | REDISMODULE_WRITE, true, false);
    if (!statusD) {
        RedisModule_CloseKey(srcKey);
        return REDISMODULE_ERR;
    }
    SeriesDeleteSrcRule(destSeries, srcKeyName);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(srcKey);
    RedisModule_CloseKey(destKey);

    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_MODULE, "ts.deleterule:src", srcKeyName);
    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_MODULE, "ts.deleterule:dest", destKeyName);

    return REDISMODULE_OK;
}

/*
TS.CREATERULE sourceKey destKey AGGREGATION aggregationType bucketDuration
*/
// aggregationType 为聚合器类型
// 为 sourceKey 创建一个压缩规则，压缩后的数据存储在 destKey 中？？？
// 创建压缩规则
// TS.CREATERULE
//              sourceKey : 为源key设置对应的数据聚合规则【必须参数】
//              destKey ： 最终聚合的数据存储到目标key中，应该在执行该命令之前创建 【必须参数】
//              AGGREGATION aggregator bucketDuration ：指定聚合器的类型以及对应的聚合周期
//              [alignTimestamp] ： 时间对齐规则，默认为0（与当前时间对齐）可以设置一个时间戳，
//                                  后续聚合的时候与设定的时间戳对齐
int TSDB_createRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 6 && argc != 7) {
        return RedisModule_WrongArity(ctx);
    }

    // Validate aggregation arguments
    // 验证聚合的相关参数
    api_timestamp_t bucketDuration;
    int aggType;
    timestamp_t alignmentTS;
    // 最终获取到bucketDuration ， aggType 和 alignmentTS
    const int result =
        _parseAggregationArgs(ctx, argv, argc, &bucketDuration, &aggType, NULL, NULL, &alignmentTS);
    if (result == TSDB_NOTEXISTS) {
        return RedisModule_WrongArity(ctx);
    }
    if (result == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    // 源key和目标key不能相等
    RedisModuleString *srcKeyName = argv[1];
    RedisModuleString *destKeyName = argv[2];
    if (!RedisModule_StringCompare(srcKeyName, destKeyName)) {
        return RTS_ReplyGeneralError(
            ctx, "TSDB: the source key and destination key should be different");
    }

    Series *srcSeries;
    RedisModuleKey *srcKey;
    const int statusS = GetSeries(
        ctx, srcKeyName, &srcKey, &srcSeries, REDISMODULE_READ | REDISMODULE_WRITE, true, false);
    if (!statusS) {
        return REDISMODULE_ERR;
    }

    // 1. Verify the source is not a destination
    // 1. 验证源key的不是一种压缩key，只有压缩类型的key的src才不是NULL
    if (srcSeries->srcKey) {
        RedisModule_CloseKey(srcKey);
        return RTS_ReplyGeneralError(ctx, "TSDB: the source key already has a source rule");
    }

    Series *destSeries;
    RedisModuleKey *destKey;
    const int statusD = GetSeries(
        ctx, destKeyName, &destKey, &destSeries, REDISMODULE_READ | REDISMODULE_WRITE, true, false);
    if (!statusD) {
        RedisModule_CloseKey(srcKey);
        return REDISMODULE_ERR;
    }

    // 2. verify dst is not s source
    // 2. 验证目标key没有一个规则，只有源key才有对应的压缩key，并且压缩key是没有规则的
    if (destSeries->rules) {
        RedisModule_CloseKey(srcKey);
        RedisModule_CloseKey(destKey);
        return RTS_ReplyGeneralError(ctx, "TSDB: the destination key already has a dst rule");
    }

    // 3. verify dst doesn't already have src,
    // 4. This covers also the scenario when the rule is already exists
    // 3. 验证目标key的srcKey不存在，确保目标key之前没有关联的源key，也就是说目标key此时还是一个空的key
    // 4. 这也涵盖了规则已经存在的场景，确保目标key之前不是其他key的压缩（规则）key
    if (destSeries->srcKey) {
        RedisModule_CloseKey(srcKey);
        RedisModule_CloseKey(destKey);
        return RTS_ReplyGeneralError(ctx, "TSDB: the destination key already has a src rule");
    }

    // add src to dest
    // 将目标key中的src的名字设置为源key的名字，通过这种方式将源key和压缩key关联了起来
    SeriesSetSrcRule(ctx, destSeries, srcSeries->keyName);

    // Last add the rule to source
    // 创建一个规则，并将这个规则加入到源key的规则列表中
    if (SeriesAddRule(ctx, srcSeries, destSeries, aggType, bucketDuration, alignmentTS) == NULL) {
        RedisModule_CloseKey(srcKey);
        RedisModule_CloseKey(destKey);
        RedisModule_ReplyWithSimpleString(ctx, "TSDB: ERROR creating rule");
        return REDISMODULE_ERR;
    }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);

    RedisModule_CloseKey(srcKey);
    RedisModule_CloseKey(destKey);

    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_MODULE, "ts.createrule:src", srcKeyName);
    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_MODULE, "ts.createrule:dest", destKeyName);

    return REDISMODULE_OK;
}


/*
TS.INCRBY ts_key NUMBER [TIMESTAMP timestamp]
*/

// 减少具有最大现有时间戳的样本的值，或者创建一个新样本，其值等于具有给定减量的最大现有时间戳的样本的值。

// ts.decrby 含义为：
//   1. 将现有样本中的最大时间戳的value变小
//   2. 新增加一个样本，样本的value等于 当前样本集中最大value的加或减对应的value大小

// TS.DECRBY/INCRBY
//         key : 对应的时序键名【必须参数】
//         value ：对应的样本数值，需要增加或者减少的值【必须参数】
//         [TIMESTAMP timestamp] ：以毫秒为单位的时间戳，当为*时，使用系统当前时间戳 【可选参数】
//                                 时间戳必须等于或高于最大现有时间戳，未指定时：根据服务器的时钟设置时间戳
//                                   当相等时：具有最大现有时间戳的样本的值减小；
//                                   当更高时：将创建一个时间戳设置为timestamp的新样本，
//                                            其值将设置为具有最大现有时间戳减去value的样本的值
//         [RETENTION retentionPeriod] ：最大保留期，与最大现有时间戳（以毫秒为单位）相比，仅在创建新时间序列时使用【可选参数】
//         [UNCOMPRESSED] ： 将数据存储从压缩（默认）更改为未压缩，仅在创建新时间序列时使用【可选参数】
//         [CHUNK_SIZE size] ： 为每个数据块分配的内存大小，以字节为单位，仅在创建新时间序列时使用【可选参数】
//         [LABELS {label value}...] ： 表示键的元数据标签并用作二级索引，仅在创建新时间序列时使用【可选参数】
int TSDB_incrby(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    Series *series;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        CreateCtx cCtx = { 0 };
        if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, &cCtx, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, cCtx.labels, cCtx.labelsCount);
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        return RTS_ReplyGeneralError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    series = RedisModule_ModuleTypeGetValue(key);

    double incrby = 0;
    if (RMUtil_ParseArgs(argv, argc, 2, "d", &incrby) != REDISMODULE_OK) {
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid increase/decrease value");
    }

    // 解析时间戳
    long long currentUpdatedTime = -1;
    int timestampLoc = RMUtil_ArgIndex("TIMESTAMP", argv, argc);
    if (timestampLoc == -1 || RMUtil_StringEqualsC(argv[timestampLoc + 1], "*")) {
        currentUpdatedTime = RedisModule_Milliseconds();
    } else if (RedisModule_StringToLongLong(argv[timestampLoc + 1],
                                            (long long *)&currentUpdatedTime) != REDISMODULE_OK) {
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp");
    }

    // 如果设置的时间戳较小，则报错，不应该执行
    if (currentUpdatedTime < series->lastTimestamp && series->lastTimestamp != 0) {
        return RedisModule_ReplyWithError(
            ctx, "TSDB: for incrby/decrby, timestamp should be newer than the lastest one");
    }

    double result = series->lastValue;
    RMUtil_StringToLower(argv[0]);
    bool isIncr = RMUtil_StringEqualsC(argv[0], "ts.incrby");
    if (isIncr) {
        result += incrby;
    } else {
        result -= incrby;
    }

    
    int rv = internalAdd(ctx, series, currentUpdatedTime, result, DP_LAST);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(key);

    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_GENERIC, isIncr ? "ts.incrby" : "ts.decrby", argv[1]);

    return rv;
}

// TS.GET key [LATEST]
// 获取最后一个样本，当前版本中暂不支持 LATEST 可选项
int TSDB_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleKey *key;
    const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ, false, false);
    if (!status) {
        return REDISMODULE_ERR;
    }

    ReplyWithSeriesLastDatapoint(ctx, series);
    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

// 获取与特定过滤器匹配的最后一个样本，在 Redis 集群上运行时，MGET 命令不能作为事务的一部分
// TS.MGET
//        [LATEST] : 暂不支持该参数
//        [WITHLABELS] : 在回复中包含表示时间序列元数据标签的所有标签值对，【可选参数】
//                       TODO：与SELECTED_LABELS只能二选一？？
//        [SELECTED_LABELS label...] ：在回复中包含表示时间序列元数据标签的标签值对的子集【可选参数】
//                                     每个系列有大量标签时，这很有用，但只需要一些标签的值
//         FILTER filter... ： 无论何时需要提供过滤器，都必须应用至少一个标签=值过滤器
//                             可能的过滤器列表：
//                                label=val ： 过滤指定值的标签
//                                label!=val ： 过滤标签不等于特定值
//                                label= -键没有标签标签 TODO: ？？？
//                                label!= -键有标签标签 TODO: ？？？
//                                label=(value1 ,value2, ... ) 带有标签标签的键等于列表中的值之一
//                                lable!=(value1 ,value2, ... ) 标签标签不等于列表中任何值的键
// 返回值：
//      键名
//      标签值对列表
//          默认情况下，报告一个空列表
//          如果WITHLABELS指定，则报告与此时间序列关联的所有标签
//          如果指定了SELECTED_LABELS label ...，则报告选定的标签
//      最后一个样本的时间标签值对
int TSDB_mget(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (IsMRCluster()) {
        int ctxFlags = RedisModule_GetContextFlags(ctx);

        if (ctxFlags & (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI |
                        REDISMODULE_CTX_FLAGS_DENY_BLOCKING)) {
            RedisModule_ReplyWithError(ctx,
                                       "Can not run multi sharded command inside a multi exec, "
                                       "lua, or when blocking is not allowed");
            return REDISMODULE_OK;
        }
        return TSDB_mget_RG(ctx, argv, argc);
    }

    RedisModule_AutoMemory(ctx);

    MGetArgs args;
    if (parseMGetCommand(ctx, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    const char **limitLabelsStr = calloc(args.numLimitLabels, sizeof(char *));
    for (int i = 0; i < args.numLimitLabels; i++) {
        limitLabelsStr[i] = RedisModule_StringPtrLen(args.limitLabels[i], NULL);
    }

    RedisModuleDict *result =
        QueryIndex(ctx, args.queryPredicates->list, args.queryPredicates->count);
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    Series *series;
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        const int status = GetSeries(ctx,
                                     RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                                     &key,
                                     &series,
                                     REDISMODULE_READ,
                                     false,
                                     true);
        if (!status) {
            RedisModule_Log(ctx,
                            "warning",
                            "couldn't open key or key is not a Timeseries. key=%.*s",
                            (int)currentKeyLen,
                            currentKey);
            continue;
        }
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithStringBuffer(ctx, currentKey, currentKeyLen);
        if (args.withLabels) {
            ReplyWithSeriesLabels(ctx, series);
        } else if (args.numLimitLabels > 0) {
            ReplyWithSeriesLabelsWithLimitC(ctx, series, limitLabelsStr, args.numLimitLabels);
        } else {
            RedisModule_ReplyWithArray(ctx, 0);
        }
        ReplyWithSeriesLastDatapoint(ctx, series);
        replylen++;
        RedisModule_CloseKey(key);
    }
    RedisModule_ReplySetArrayLength(ctx, replylen);
    RedisModule_DictIteratorStop(iter);
    MGetArgs_Free(&args);
    free(limitLabelsStr);
    return REDISMODULE_OK;
}

static inline bool is_obsolete(timestamp_t ts,
                               timestamp_t lastTimestamp,
                               timestamp_t retentionTime) {
    return (lastTimestamp > retentionTime) && (ts < lastTimestamp - retentionTime);
}

static inline bool verify_compaction_del_possible(RedisModuleCtx *ctx,
                                                  const Series *series,
                                                  const RangeArgs *args) {
    bool is_valid = true;
    if (!series->rules)
        return true;

    // Verify startTimestamp in retention period
    if (is_obsolete(args->startTimestamp, series->lastTimestamp, series->retentionTime)) {
        is_valid = false;
    }

    // Verify all compaction's buckets are in the retention period
    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        const timestamp_t ruleTimebucket = rule->bucketDuration;
        const timestamp_t curAggWindowStart = BucketStartNormalize(
            CalcBucketStart(args->startTimestamp, ruleTimebucket, rule->timestampAlignment));
        if (is_obsolete(curAggWindowStart, series->lastTimestamp, series->retentionTime)) {
            is_valid = false;
        }
        rule = rule->nextRule;
    }

    if (unlikely(!is_valid)) {
        RTS_ReplyGeneralError(
            ctx,
            "TSDB: Can't delete an event which is older than retention time, in such case no "
            "valid way to update the downsample");
    }

    return is_valid;
}

// TS.DEL key fromTimestamp toTimestamp
// 删除给定时间范围内的所有样本
int TSDB_delete(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    RangeArgs args = { 0 };
    if (parseRangeArguments(ctx, 2, argv, argc, (timestamp_t)0, &args) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    Series *series;
    RedisModuleKey *key;
    const int status =
        GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ | REDISMODULE_WRITE, false, false);
    if (!status) {
        return REDISMODULE_ERR;
    }

    if (unlikely(!verify_compaction_del_possible(ctx, series, &args))) {
        RedisModule_CloseKey(key);
        return REDISMODULE_ERR;
    }

    size_t deleted = SeriesDelRange(series, args.startTimestamp, args.endTimestamp);

    RedisModule_ReplyWithLongLong(ctx, deleted);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.del", argv[1]);

    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

void FlushEventCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    if ((!memcmp(&eid, &RedisModuleEvent_FlushDB, sizeof(eid))) &&
        subevent == REDISMODULE_SUBEVENT_FLUSHDB_END) {
        RemoveAllIndexedMetrics();
    }
}

void swapDbEventCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    RedisModule_Log(ctx, "warning", "swapdb isn't supported by redis timeseries");
    if ((!memcmp(&e, &RedisModuleEvent_FlushDB, sizeof(e)))) {
        RedisModuleSwapDbInfo *ei = data;
        REDISMODULE_NOT_USED(ei);
    }
}

int persistence_in_progress = 0;

void persistCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    if (memcmp(&eid, &RedisModuleEvent_Persistence, sizeof(eid)) != 0) {
        return;
    }

    if (subevent == REDISMODULE_SUBEVENT_PERSISTENCE_RDB_START ||
        subevent == REDISMODULE_SUBEVENT_PERSISTENCE_AOF_START ||
        subevent == REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START ||
        subevent == REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_AOF_START) {
        persistence_in_progress++;
    } else if (subevent == REDISMODULE_SUBEVENT_PERSISTENCE_ENDED ||
               subevent == REDISMODULE_SUBEVENT_PERSISTENCE_FAILED) {
        persistence_in_progress--;
    }

    return;
}

void ShardingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    /**
     * On sharding event we need to do couple of things depends on the subevent given:
     *
     * 1. REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED
     *    On this event we know that the slot range changed and we might have data
     *    which are no longer belong to this shard, we must ignore it on searches
     *
     * 2. REDISMODULE_SUBEVENT_SHARDING_TRIMMING_STARTED
     *    This event tells us that the trimming process has started and keys will start to be
     *    deleted, we do not need to do anything on this event
     *
     * 3. REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED
     *    This event tells us that the trimming process has finished, we are not longer
     *    have data that are not belong to us and its safe to stop checking this on searches.
     */
    if (eid.id != REDISMODULE_EVENT_SHARDING) {
        RedisModule_Log(rts_staticCtx, "warning", "Bad event given, ignored.");
        return;
    }

    switch (subevent) {
        case REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED:
            RedisModule_Log(
                ctx, "notice", "%s", "Got slot range change event, enter trimming phase.");
            isTrimming = true;
            break;
        case REDISMODULE_SUBEVENT_SHARDING_TRIMMING_STARTED:
            RedisModule_Log(
                ctx, "notice", "%s", "Got trimming started event, enter trimming phase.");
            isTrimming = true;
            break;
        case REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED:
            RedisModule_Log(ctx, "notice", "%s", "Got trimming ended event, exit trimming phase.");
            isTrimming = false;
            break;
        default:
            RedisModule_Log(rts_staticCtx, "warning", "Bad subevent given, ignored.");
    }
}

int NotifyCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    if (strcasecmp(event, "del") ==
            0 || // unlink also notifies with del with freeseries called before
        strcasecmp(event, "set") == 0 ||
        strcasecmp(event, "expired") == 0 || strcasecmp(event, "evict") == 0 ||
        strcasecmp(event, "evicted") == 0 || strcasecmp(event, "trimmed") == 0 // only on enterprise
    ) {
        RemoveIndexedMetric(key);
        return REDISMODULE_OK;
    }

    if (strcasecmp(event, "restore") == 0) {
        RestoreKey(ctx, key);
        return REDISMODULE_OK;
    }

    if (strcasecmp(event, "rename_from") == 0) { // include also renamenx
        RenameSeriesFrom(ctx, key);
        return REDISMODULE_OK;
    }

    if (strcasecmp(event, "rename_to") == 0) { // include also renamenx
        RenameSeriesTo(ctx, key);
        return REDISMODULE_OK;
    }

    // Will be called in replicaof or on load rdb on load time
    if (strcasecmp(event, "loaded") == 0) {
        IndexMetricFromName(ctx, key);
        return REDISMODULE_OK;
    }

    // if (strcasecmp(event, "short read") == 0) // Nothing should be done
    return REDISMODULE_OK;
}

void ReplicaBackupCallback(RedisModuleCtx *ctx,
                           RedisModuleEvent eid,
                           uint64_t subevent,
                           void *data) {
    REDISMODULE_NOT_USED(eid);
    switch (subevent) {
        case REDISMODULE_SUBEVENT_REPL_BACKUP_CREATE:
            Backup_Globals();
            break;
        case REDISMODULE_SUBEVENT_REPL_BACKUP_RESTORE:
            Restore_Globals();
            break;
        case REDISMODULE_SUBEVENT_REPL_BACKUP_DISCARD:
            Discard_Globals_Backup();
            break;
    }
}

bool CheckVersionForBlockedClientMeasureTime() {
    // Minimal versions: 6.2.0
    if (RTS_currVersion.redisMajorVersion >= 6 && RTS_currVersion.redisMinorVersion >= 2) {
        return true;
    } else {
        return false;
    }
}

int CheckVersionForShortRead() {
    // Minimal versions: 6.2.5
    // (6.0.15 is not supporting the required event notification for modules)
    if (RTS_currVersion.redisMajorVersion == 6 && RTS_currVersion.redisMinorVersion == 2) {
        return RTS_currVersion.redisPatchVersion >= 5 ? REDISMODULE_OK : REDISMODULE_ERR;
    } else if (RTS_currVersion.redisMajorVersion == 255 &&
               RTS_currVersion.redisMinorVersion == 255 &&
               RTS_currVersion.redisPatchVersion == 255) {
        // Also supported on master (version=255.255.255)
        return REDISMODULE_OK;
    }
    return REDISMODULE_ERR;
}

void Initialize_RdbNotifications(RedisModuleCtx *ctx) {
    if (CheckVersionForShortRead() == REDISMODULE_OK) {
        int success = RedisModule_SubscribeToServerEvent(
            ctx, RedisModuleEvent_ReplBackup, ReplicaBackupCallback);
        RedisModule_Assert(success !=
                           REDISMODULE_ERR); // should be supported in this redis version/release
        RedisModule_SetModuleOptions(ctx, REDISMODULE_OPTIONS_HANDLE_IO_ERRORS);
        RedisModule_Log(ctx, "notice", "Enabled diskless replication");
    }
}

/*
module loading function, possible arguments:
COMPACTION_POLICY - compaction policy from parse_policies,h
RETENTION_POLICY - long that represents the retention in milliseconds
MAX_SAMPLE_PER_CHUNK - how many samples per chunk
example:
redis-server --loadmodule ./redistimeseries.so COMPACTION_POLICY
"max:1m:1d;min:10s:1h;avg:2h:10d;avg:3d:100d" RETENTION_POLICY 3600 MAX_SAMPLE_PER_CHUNK 1024
*/
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "timeseries", REDISTIMESERIES_MODULE_VERSION, REDISMODULE_APIVER_1) ==
        REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    rts_staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);

    RedisModule_Log(ctx,
                    "notice",
                    "RedisTimeSeries version %d, git_sha=%s",
                    REDISTIMESERIES_MODULE_VERSION,
                    REDISTIMESERIES_GIT_SHA);

    RTS_GetRedisVersion();
    RedisModule_Log(ctx,
                    "notice",
                    "Redis version found by RedisTimeSeries : %d.%d.%d - %s",
                    RTS_currVersion.redisMajorVersion,
                    RTS_currVersion.redisMinorVersion,
                    RTS_currVersion.redisPatchVersion,
                    RTS_IsEnterprise() ? "enterprise" : "oss");
    if (RTS_IsEnterprise()) {
        RedisModule_Log(ctx,
                        "notice",
                        "Redis Enterprise version found by RedisTimeSeries : %d.%d.%d-%d",
                        RTS_RlecMajorVersion,
                        RTS_RlecMinorVersion,
                        RTS_RlecPatchVersion,
                        RTS_RlecBuild);
    }

    if (RTS_CheckSupportedVestion() != REDISMODULE_OK) {
        RedisModule_Log(ctx,
                        "warning",
                        "Redis version is to old, please upgrade to redis "
                        "%d.%d.%d and above.",
                        RTS_minSupportedVersion.redisMajorVersion,
                        RTS_minSupportedVersion.redisMinorVersion,
                        RTS_minSupportedVersion.redisPatchVersion);
        return REDISMODULE_ERR;
    }

    if (ReadConfig(ctx, argv, argc) == TSDB_ERROR) {
        RedisModule_Log(
            ctx, "warning", "Failed to parse RedisTimeSeries configurations. aborting...");
        return REDISMODULE_ERR;
    }

    initGlobalCompactionFunctions();

    if (register_rg(ctx, TSGlobalConfig.numThreads) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleTypeMethods tm = { .version = REDISMODULE_TYPE_METHOD_VERSION,
                                  .rdb_load = series_rdb_load,
                                  .rdb_save = series_rdb_save,
                                  .aof_rewrite = RMUtil_DefaultAofRewrite,
                                  .mem_usage = SeriesMemUsage,
                                  .copy = CopySeries,
                                  .free = FreeSeries };

    SeriesType = RedisModule_CreateDataType(ctx, "TSDB-TYPE", TS_LATEST_ENCVER, &tm);
    if (SeriesType == NULL)
        return REDISMODULE_ERR;
    IndexInit();
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.create", TSDB_create);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.alter", TSDB_alter);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.createrule", TSDB_createRule);
    RMUtil_RegisterWriteCmd(ctx, "ts.deleterule", TSDB_deleteRule);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.add", TSDB_add);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.incrby", TSDB_incrby);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.decrby", TSDB_incrby);
    RMUtil_RegisterReadCmd(ctx, "ts.range", TSDB_range);
    RMUtil_RegisterReadCmd(ctx, "ts.revrange", TSDB_revrange);

    if (RedisModule_CreateCommand(ctx, "ts.queryindex", TSDB_queryindex, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RMUtil_RegisterReadCmd(ctx, "ts.info", TSDB_info);
    RMUtil_RegisterReadCmd(ctx, "ts.get", TSDB_get);
    RMUtil_RegisterWriteCmd(ctx, "ts.del", TSDB_delete);

    if (RedisModule_CreateCommand(ctx, "ts.madd", TSDB_madd, "write deny-oom", 1, -1, 3) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "ts.mrange", TSDB_mrange, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "ts.mrevrange", TSDB_mrevrange, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "ts.mget", TSDB_mget, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RedisModule_SubscribeToKeyspaceEvents(
        ctx,
        REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_SET | REDISMODULE_NOTIFY_STRING |
            REDISMODULE_NOTIFY_EVICTED | REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_LOADED |
            REDISMODULE_NOTIFY_TRIMMED,
        NotifyCallback);

    if (RedisModule_SubscribeToServerEvent && RedisModule_ShardingGetKeySlot) {
        // we have server events support, lets subscribe to relevan events.
        RedisModule_Log(ctx, "notice", "%s", "Subscribe to sharding events");
        RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Sharding, ShardingEvent);
    }

    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, FlushEventCallback);
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_SwapDB, swapDbEventCallback);
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Persistence, persistCallback);

    Initialize_RdbNotifications(ctx);

    return REDISMODULE_OK;
}
