# pq_getmsgbyte

## Location
src/backend/libpq/pqformat.c: 399 - 414

## Overview
Extracts a single byte from a message buffer and advances the cursor position.

## Definition


## Detailed Description
The  function is a fundamental message parsing utility in PostgreSQL's libpq communication protocol. It reads one raw byte from a message buffer (StringInfo) at the current cursor position and automatically advances the cursor for subsequent reads. The function includes bounds checking to ensure data integrity during message parsing operations. If the cursor has reached or exceeded the message length, it raises a protocol violation error rather than allowing buffer overruns.

## Parameters / Member Variables
- : A StringInfo structure containing the message buffer data, length, and current cursor position

## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting mechanism)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
- Called from (representative examples):
  - [HandleParallelMessage](../H/HandleParallelMessage.md)
  - [logicalrep_read_commit](../l/logicalrep_read_commit.md)
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md)
  - [boolrecv](../b/boolrecv.md)
  - [charrecv](../c/charrecv.md)
  - [macaddr_recv](../m/macaddr_recv.md)
  - [network_recv](../n/network_recv.md)

## Notes and Other Information
- Returns the byte value as an int (cast from unsigned char)
- Automatically increments the cursor position after reading
- Part of PostgreSQL's binary message format parsing infrastructure
- Used extensively in logical replication, parallel processing, and data type deserialization
- Throws ERRCODE_PROTOCOL_VIOLATION if attempting to read beyond message boundaries
- The function is defined in src/backend/libpq/pqformat.c:399-414