-- Check table parts (should be < 100 per partition)
SELECT 
    table,
    partition,
    count() AS parts_count,
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk
FROM system.parts
WHERE database = 'bikeshare' AND active
GROUP BY table, partition
ORDER BY parts_count DESC;

-- Identify slow queries
SELECT 
    substring(query, 1, 100) AS query_preview,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    query_duration_ms,
    formatReadableSize(memory_usage) AS memory_used
FROM system.query_log
WHERE event_time > now() - INTERVAL 1 DAY
  AND type = 'QueryFinish'
  AND query NOT LIKE '%system.%'
  AND query_duration_ms > 1000
ORDER BY query_duration_ms DESC
LIMIT 10;

-- Check projection usage
SELECT 
    table,
    name AS projection_name,
    formatReadableSize(bytes_on_disk) AS size,
    rows
FROM system.projection_parts
WHERE database = 'bikeshare' AND active
ORDER BY table, name;

-- Check compression ratios
SELECT 
    table,
    column,
    type,
    compression_codec,
    formatReadableSize(data_compressed_bytes) AS compressed,
    formatReadableSize(data_uncompressed_bytes) AS uncompressed,
    round(data_uncompressed_bytes / data_compressed_bytes, 2) AS compression_ratio
FROM system.columns
WHERE database = 'bikeshare' 
  AND table = 'raw_rides'
  AND data_compressed_bytes > 0
ORDER BY data_compressed_bytes DESC;