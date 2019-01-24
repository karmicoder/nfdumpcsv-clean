CREATE TABLE netflow (
  start_time DateTime,
  end_time DateTime,
  dur UInt32,
  src_ip FixedString(16),
  src_port UInt16,
  dst_ip FixedString(16),
  dst_port UInt16,
  protocol UInt8,
  packets_in UInt32,
  bytes_in UInt64,
  packets_out UInt32,
  bytes_out UInt64
) ENGINE = MergeTree
PARTITION BY start_time
ORDER BY start_time