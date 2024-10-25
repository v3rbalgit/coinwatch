-- Check partition information
SELECT
    partition_name,
    partition_description,
    table_rows,
    data_length/1024/1024 as data_size_mb,
    index_length/1024/1024 as index_size_mb
FROM information_schema.partitions
WHERE table_schema = DATABASE()
AND table_name = 'kline_data'
ORDER BY partition_ordinal_position;

-- Check data distribution across partitions
SELECT
    p.partition_name,
    MIN(k.start_time) as min_timestamp,
    MAX(k.start_time) as max_timestamp,
    COUNT(*) as record_count,
    FROM_UNIXTIME(MIN(k.start_time/1000)) as min_date,
    FROM_UNIXTIME(MAX(k.start_time/1000)) as max_date
FROM kline_data k
PARTITION (p_historical)
GROUP BY p.partition_name
UNION ALL
SELECT
    'p_recent',
    MIN(k.start_time),
    MAX(k.start_time),
    COUNT(*),
    FROM_UNIXTIME(MIN(k.start_time/1000)),
    FROM_UNIXTIME(MAX(k.start_time/1000))
FROM kline_data k
PARTITION (p_recent)
UNION ALL
SELECT
    'p_current',
    MIN(k.start_time),
    MAX(k.start_time),
    COUNT(*),
    FROM_UNIXTIME(MIN(k.start_time/1000)),
    FROM_UNIXTIME(MAX(k.start_time/1000))
FROM kline_data k
PARTITION (p_current);

-- Check for any data outside expected partition ranges
SELECT
    FROM_UNIXTIME(start_time/1000) as data_time,
    start_time,
    COUNT(*) as record_count
FROM kline_data
WHERE start_time < (
    SELECT CAST(partition_description AS SIGNED)
    FROM information_schema.partitions
    WHERE table_schema = DATABASE()
    AND table_name = 'kline_data'
    AND partition_name = 'p_historical'
)
GROUP BY start_time
ORDER BY start_time DESC;