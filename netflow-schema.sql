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
  bytes_out UInt64,
  src_ip_str String MATERIALIZED replaceRegexpOne(IPv6NumToString(src_ip), '^::ffff:', '')
  dst_ip_str String MATERIALIZED replaceRegexpOne(IPv6NumToSTring(dst_ip), '^::ffff:', '')
) ENGINE = MergeTree
PARTITION BY start_time
ORDER BY start_time