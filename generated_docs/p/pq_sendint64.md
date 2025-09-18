# pq_sendint64

## Location
src/include/libpq/pqformat.h: 152 - 159

## Overview
Appends a binary 64-bit unsigned integer to a StringInfo buffer for network transmission in PostgreSQL protocol messages.

## Definition
```c
static inline void pq_sendint64(StringInfo buf, uint64 i)
```

## Detailed Description
`pq_sendint64` is an inline function that appends a 64-bit unsigned integer to a StringInfo buffer. This function is essential for handling large values in PostgreSQL's protocol layer, particularly for timestamps, transaction IDs, LSN values, and large numeric types. It is heavily used in logical replication for commit/prepare timestamps, WAL sender operations for LSN transmission, and data type serialization for 64-bit values. The function follows the standard pattern of ensuring buffer capacity through `enlargeStringInfo` before writing the integer in network byte order using `pq_writeint64`.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the 64-bit integer will be appended
- `i`: The 64-bit unsigned integer value to append to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - enlargeStringInfo
  - pq_writeint64
- Called from (representative examples):
  - logicalrep_write_begin
  - logicalrep_write_commit
  - logicalrep_write_prepare_common
  - send_feedback
  - XLogWalRcvSendReply
  - WalSndPrepareWrite
  - XLogSendPhysical
  - timestamp_send
  - int8send
  - numeric_avg_serialize
  - pg_lsn_send

## Notes and Other Information
- Defined as a static inline function for optimal performance
- Critical for logical replication protocol messages containing timestamps and LSNs
- Essential for WAL sender/receiver communication in streaming replication
- Used extensively for 64-bit data type serialization (timestamps, bigint, money, etc.)
- Handles network byte order conversion automatically
- The 64-bit capacity is necessary for PostgreSQL's high-precision timestamps and large transaction identifiers
- Vital for maintaining consistency in distributed PostgreSQL environments
- Used in both logical and physical replication protocols