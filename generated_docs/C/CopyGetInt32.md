# CopyGetInt32

## Location
[src/backend/commands/copyfromparse.c:362-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L362-L378)

## Overview
Reads a 32-bit integer from binary COPY data with automatic conversion from network byte order to host byte order.

## Definition
static inline bool CopyGetInt32(CopyFromState cstate, int32 *val)

## Detailed Description
CopyGetInt32 is a utility function for reading 32-bit integers from binary COPY data streams. It reads exactly 4 bytes of binary data and converts the value from network byte order (big-endian) to the host machine's byte order using pg_ntoh32(). This ensures that binary COPY data can be correctly interpreted regardless of the endianness of the source and destination systems. The function provides error handling by returning false if the required number of bytes cannot be read, indicating EOF or data corruption.

## Parameters / Member Variables
- `cstate`: CopyFromState structure containing the current state and configuration of the COPY operation, used to read binary data from the input source
- `val`: Pointer to int32 variable where the converted integer value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [CopyReadBinaryData](CopyReadBinaryData.md) (read raw binary data from input)
  - pg_ntoh32 (convert from network to host byte order)
- Called from (representative examples):
  - [ReceiveCopyBinaryHeader](../R/ReceiveCopyBinaryHeader.md) (src/backend/commands/copyfromparse.c:202, 216)
  - [CopyReadBinaryAttribute](CopyReadBinaryAttribute.md) (src/backend/commands/copyfromparse.c:1993)

## Notes and Other Information
- Returns true on successful read, false on EOF or insufficient data
- Automatically handles byte order conversion for cross-platform compatibility
- Sets the output value to 0 on failure to suppress compiler warnings
- Used primarily for reading length fields and other metadata in binary COPY format
- The function is declared as static inline for performance optimization