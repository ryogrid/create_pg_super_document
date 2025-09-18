# CopySendInt32

## Location
src/backend/commands/copyto.c: 265 - 276

## Overview
Sends a 32-bit integer value in network byte order as part of PostgreSQL's binary COPY format operations.

## Definition
```c
static inline void CopySendInt32(CopyToState cstate, int32 val)
```

## Detailed Description
CopySendInt32 is a utility function used in PostgreSQL's binary COPY TO operations to transmit 32-bit integer values. It converts the input integer from host byte order to network byte order (big-endian) using pg_hton32() before sending the data through CopySendData(). This ensures consistent byte ordering across different platforms when writing binary COPY data, which is crucial for portability and data interchange.

The function is declared as static inline for performance optimization, as it's a simple operation that's called frequently during binary COPY operations.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing the current state of the COPY operation
- `val`: The 32-bit integer value to be sent in network byte order

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton32 (host-to-network byte order conversion)
  - [CopySendData](CopySendData.md) (low-level data transmission function)
- Called from (representative examples):
  - DR_copy
  - [DoCopyTo](../D/DoCopyTo.md)
  - [CopyOneRowTo](CopyOneRowTo.md)

## Notes and Other Information
- This function is specifically used for binary COPY format operations, not text format
- The inline declaration optimizes performance for this frequently-called utility function
- Network byte order (big-endian) ensures cross-platform compatibility of binary COPY data
- Part of the binary COPY protocol implementation that handles type-specific data serialization