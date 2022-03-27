### TS.ADD

Append a new sample to the series. If the series has not been created yet with `TS.CREATE` it will be automatically created. 

```sql
TS.ADD key timestamp value [RETENTION retentionTime] [ENCODING [COMPRESSED|UNCOMPRESSED]] [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS {label value}...]
```

* timestamp - (integer) UNIX sample timestamp **in milliseconds**. `*` can be used for an automatic timestamp from the system clock.
* value - (double) numeric data value of the sample. We expect the double number to follow [RFC 7159](https://tools.ietf.org/html/rfc7159) (JSON standard). In particular, the parser will reject overly large values that would not fit in binary64. It will not accept NaN or infinite values.

The following arguments are optional because they can be set by TS.CREATE:

 * RETENTION _retentionTime_ - Maximum age for samples compared to last event time (in milliseconds). Relevant only when adding data to a time series that hasn't been previously created; when adding samples to an existing time series, this argument is ignored.
    * Default: The global retention secs configuration of the database (by default, `0`)
    * When set to 0, the series is not trimmed at all
 * ENCODING _enc_ - Specify the series samples encoding format. One of the following values:
    * `COMPRESSED`: apply the DoubleDelta compression to the series samples, meaning compression of Delta of Deltas between timestamps and compression of values via XOR encoding.
    * `UNCOMPRESSED`: keep the raw samples in memory.
 * CHUNK_SIZE _size_ - amount of memory, in bytes, allocated for data. Must be a multiple of 8, Default: 4096.
 * ON_DUPLICATE _policy_ - overwrite key and database configuration for `DUPLICATE_POLICY` (policy for handling multiple samples with identical timestamps). One of the following values:
   * `BLOCK` - an error will occur for any out of order sample
   * `FIRST` - ignore any newly reported value
   * `LAST` - override with the newly reported value
   * `MIN` - only override if the value is lower than the existing value
   * `MAX` - only override if the value is higher than the existing value
   * `SUM` - If a previous sample exists, add the new sample to it so that the updated value is equal to (previous + new). If no previous sample exists, set the updated value equal to the new value.
 * LABELS {_label_ _value_...} - Set of label-value pairs that represent metadata labels of the key. Relevant only when adding data to a time series that hasn't been previously created; when adding samples to an existing time series, this argument is ignored.

#### Examples
```sql
127.0.0.1:6379>TS.ADD temperature:2:32 1548149180000 26 LABELS sensor_id 2 area_id 32
(integer) 1548149180000
127.0.0.1:6379>TS.ADD temperature:3:11 1548149183000 27 RETENTION 3600
(integer) 1548149183000
127.0.0.1:6379>TS.ADD temperature:3:11 * 30
(integer) 1559718352000
```

#### Complexity

If a compaction rule exits on a time series, `TS.ADD` performance might be reduced.
The complexity of `TS.ADD` is always O(M) when M is the number of compaction rules or O(1) with no compaction.

#### Notes

- You can use this command to add data to a nonexisting time series in a single command.
  This is why `RETENTION` and `LABELS` are optional arguments.
- When specified and the key doesn't exist, RedisTimeSeries will create the key with the specified `RETENTION` and or `LABELS`.
  Setting `RETENTION` and `LABELS` introduces additional time complexity.
- Updating a sample in a trimmed window will update down-sampling aggregation based on the existing data.