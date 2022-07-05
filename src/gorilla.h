/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#ifndef GORILLA_H
#define GORILLA_H

#include "consts.h"
#include "generic_chunk.h"

#include <stdbool.h>   // bool
#include <sys/types.h> // u_int_t

typedef u_int64_t timestamp_t;
typedef u_int64_t binary_t;
typedef u_int64_t globalbit_t;
typedef u_int8_t localbit_t;

typedef union
{
    double d;
    int64_t i;
    u_int64_t u;
} union64bits;

typedef struct CompressedChunk
{
    u_int64_t size; // chunk的总大小
    u_int64_t count; // 内部包含的样本的数量，创建chunk之后，插入第一个样本的时候count为0
    u_int64_t idx; // 代表当前数据已经插入的哪个位置了，size - idx 意味着还剩余多大的空间可用于存储数据

    union64bits baseValue; // 插入的第一个样本的value
    u_int64_t baseTimestamp; // 插入的第一个样本的时间戳

    u_int64_t *data; // 最终记录数据的数据集？？？

    u_int64_t prevTimestamp; // 上一次插入数据的时间戳
    int64_t prevTimestampDelta; // ？？？

    union64bits prevValue;
    u_int8_t prevLeading;
    u_int8_t prevTrailing;
} CompressedChunk;

typedef struct Compressed_Iterator
{
    CompressedChunk *chunk;
    u_int64_t idx;
    u_int64_t count;

    // timestamp vars
    u_int64_t prevTS;
    int64_t prevDelta;

    // value vars
    union64bits prevValue;
    u_int8_t leading;
    u_int8_t trailing;
    u_int8_t blocksize;
} Compressed_Iterator;

ChunkResult Compressed_Append(CompressedChunk *chunk, u_int64_t timestamp, double value);
ChunkResult Compressed_ChunkIteratorGetNext(ChunkIter_t *iter, Sample *sample);

#endif
