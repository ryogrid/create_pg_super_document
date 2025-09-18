# CopySendInt16

## Location
src/backend/commands/copyto.c: 277 - 288

## Overview
Sends a 16-bit integer value in network byte order as part of PostgreSQL's binary COPY format operations.

## Definition
```c
static inline void CopySendInt16(CopyToState cstate, int16 val)
```

## Detailed Description
CopySendInt16 is a utility function used in PostgreSQL's binary COPY TO operations to transmit 16-bit integer values. Similar to its 32-bit counterpart, it converts the input integer from host byte order to network byte order (big-endian) using pg_hton16() before sending the data through CopySendData(). This ensures consistent byte ordering across different platforms when writing binary COPY data.

The function is declared as static inline for performance optimization, as it's a simple operation that may be called frequently during binary COPY operations, particularly for metadata fields and smaller integer columns.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing the current state of the COPY operation
- `val`: The 16-bit integer value to be sent in network byte order

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton16 (host-to-network byte order conversion for 16-bit values)
  - CopySendData (low-level data transmission function)
- Called from (representative examples):
  - DR_copy
  - DoCopyTo
  - CopyOneRowTo

## Notes and Other Information
- This function is specifically used for binary COPY format operations, not text format
- The inline declaration optimizes performance for this utility function
- Network byte order (big-endian) ensures cross-platform compatibility of binary COPY data
- Commonly used for sending field counts, attribute numbers, and other metadata in binary COPY format
- Part of the binary COPY protocol implementation that handles type-specific data serialization