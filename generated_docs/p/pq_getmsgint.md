# pq_getmsgint

## Location
src/backend/libpq/pqformat.c: 415 - 452

## Overview
Extracts a binary integer of specified byte size from a message buffer, handling network byte order conversion.

## Definition


## Detailed Description
The `pq_getmsgint` function reads a binary integer from a message buffer with automatic network-to-host byte order conversion. It supports 1, 2, and 4-byte integers, treating all values as unsigned. The function uses appropriate byte-swapping operations (pg_ntoh16, pg_ntoh32) to convert from network byte order to host byte order, ensuring correct interpretation across different machine architectures. This is essential for PostgreSQL's cross-platform binary protocol communication.

## Parameters / Member Variables
- `msg`: A StringInfo structure containing the message buffer data, length, and current cursor position
- `b`: The byte size of the integer to read (1, 2, or 4 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_copymsgbytes](pq_copymsgbytes.md) (copies bytes from message buffer)
  - pg_ntoh16 (network-to-host 16-bit conversion)
  - pg_ntoh32 (network-to-host 32-bit conversion)
  - elog (error logging for unsupported sizes)
- Called from (representative examples):
  - [HandleParallelMessage](../H/HandleParallelMessage.md)
  - [logicalrep_read_begin](../l/logicalrep_read_begin.md)
  - [exec_bind_message](../e/exec_bind_message.md)
  - [array_recv](../a/array_recv.md)
  - [numeric_recv](../n/numeric_recv.md)
  - [pq_getmsgfloat4](pq_getmsgfloat4.md)

## Notes and Other Information
- Supports only 1, 2, and 4-byte integers; other sizes trigger an error
- Always returns unsigned int, regardless of input size
- Automatically handles network byte order conversion for multi-byte values
- Part of PostgreSQL's binary message format parsing infrastructure
- Used extensively in logical replication, function calls, and data type deserialization
- The function is defined in src/backend/libpq/pqformat.c:415-452
- Single-byte reads do not require byte order conversion