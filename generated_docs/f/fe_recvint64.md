# fe_recvint64

## Location
src/bin/pg_basebackup/streamutil.c: 934 - 941

## Overview
Converts a 64-bit integer from network byte order to native byte order after receiving it from network protocols.

## Definition
```c
int64 fe_recvint64(char *buf)
```

## Detailed Description
This function reads an 8-byte value from a buffer containing a 64-bit integer in network byte order (big-endian) and converts it to the native host byte order. This is the counterpart to fe_sendint64 and is used in PostgreSQL's streaming utilities to process incoming network data. The function ensures consistent interpretation of 64-bit values received over the network regardless of the host architecture.

The function uses PostgreSQL's portable byte-swapping infrastructure (pg_ntoh64) to handle the conversion, which automatically performs the necessary byte order conversion for the current platform.

## Parameters / Member Variables
- `buf`: Input buffer containing the 8-byte network byte order value to convert

## Dependencies
- Functions called/Symbols referenced:
  - pg_ntoh64 (converts network to host byte order for 64-bit values)
- Called from (representative examples):
  - StreamLogicalLog (in pg_recvlogical.c)
  - ProcessXLogDataMsg (in receivelog.c)

## Notes and Other Information
- The function assumes the input buffer contains at least 8 bytes of valid data
- Returns the converted 64-bit signed integer in native host byte order
- Used primarily in PostgreSQL's streaming replication protocol for receiving timestamps, LSN values, and other 64-bit quantities
- Part of the frontend utility functions for pg_basebackup and related tools
- Declared in streamutil.h as part of the streaming utilities interface
- Commonly used for processing replication protocol messages and logical replication data