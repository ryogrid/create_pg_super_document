# fe_sendint64

## Location
src/bin/pg_basebackup/streamutil.c: 923 - 933

## Overview
Converts a 64-bit integer from native byte order to network byte order for transmission over network protocols.

## Definition


## Detailed Description
This function takes a 64-bit signed integer in the native host byte order and converts it to network byte order (big-endian) for transmission over the network. The converted value is stored in the provided buffer. This is part of PostgreSQL's streaming utility functions used in logical replication and base backup operations to ensure consistent data representation across different architectures.

The function uses PostgreSQL's portable byte-swapping infrastructure () to handle the conversion, which automatically handles the byte order conversion regardless of the host architecture.

## Parameters / Member Variables
- : The 64-bit signed integer to convert to network byte order
- : Output buffer where the converted 8-byte value will be stored (must be at least 8 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton64 (converts host to network byte order for 64-bit values)
- Called from (representative examples):
  - [sendFeedback](../s/sendFeedback.md) (in pg_recvlogical.c and receivelog.c)

## Notes and Other Information
- The function assumes the output buffer has sufficient space (8 bytes) to store the converted value
- Used primarily in PostgreSQL's streaming replication protocol for sending timestamps, LSN values, and other 64-bit quantities
- Part of the frontend utility functions for pg_basebackup and related tools
- Declared in streamutil.h as part of the streaming utilities interface