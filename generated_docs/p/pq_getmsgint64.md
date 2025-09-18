# pq_getmsgint64

## Location
src/backend/libpq/pqformat.c: 453 - 468

## Overview
Extracts a 64-bit binary integer from a message buffer with network-to-host byte order conversion.

## Definition


## Detailed Description
The `pq_getmsgint64` function reads a fixed 8-byte (64-bit) integer from a message buffer and performs network-to-host byte order conversion using pg_ntoh64. This function is specifically designed for 64-bit integers and is kept separate from pq_getmsgint for performance reasons, as forcing all integer operations to use 64-bit arithmetic could impact performance on systems where 64-bit operations are less efficient. The function is essential for handling large integers, timestamps, LSNs, and other 64-bit values in PostgreSQL's binary protocol.

## Parameters / Member Variables
- `msg`: A StringInfo structure containing the message buffer data, length, and current cursor position

## Dependencies
- Functions called/Symbols referenced:
  - [pq_copymsgbytes](pq_copymsgbytes.md) (copies 8 bytes from message buffer)
  - pg_ntoh64 (network-to-host 64-bit conversion)
- Called from (representative examples):
  - [HandleParallelMessage](../H/HandleParallelMessage.md)
  - [logicalrep_read_begin](../l/logicalrep_read_begin.md)
  - [logicalrep_read_commit](../l/logicalrep_read_commit.md)
  - [XLogWalRcvProcessMsg](../X/XLogWalRcvProcessMsg.md)
  - [ProcessStandbyReplyMessage](../P/ProcessStandbyReplyMessage.md)
  - [timestamp_recv](../t/timestamp_recv.md)
  - [int8recv](../i/int8recv.md)
  - [pq_getmsgfloat8](pq_getmsgfloat8.md)

## Notes and Other Information
- Always reads exactly 8 bytes from the message buffer
- Returns signed 64-bit integer (int64)
- Automatically handles network byte order conversion for cross-platform compatibility
- Designed as a separate function from pq_getmsgint for performance optimization
- Used extensively in logical replication for timestamps, LSNs, and transaction IDs
- Essential for WAL receiver/sender message processing
- The function is defined in src/backend/libpq/pqformat.c:453-468
- Part of PostgreSQL's binary message format parsing infrastructure