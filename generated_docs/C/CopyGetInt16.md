# CopyGetInt16

## Location
[src/backend/commands/copyfromparse.c:379-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L379-L399)

## Overview
Reads a 16-bit integer from binary COPY data with automatic conversion from network byte order to host byte order.

## Definition
static inline bool CopyGetInt16(CopyFromState cstate, int16 *val)

## Detailed Description
CopyGetInt16 is a utility function for reading 16-bit integers from binary COPY data streams. It reads exactly 2 bytes of binary data and converts the value from network byte order (big-endian) to the host machine's byte order using pg_ntoh16(). This ensures that binary COPY data can be correctly interpreted regardless of the endianness of the source and destination systems. The function provides error handling by returning false if the required number of bytes cannot be read, indicating EOF or data corruption.

## Parameters / Member Variables
- `cstate`: CopyFromState structure containing the current state and configuration of the COPY operation, used to read binary data from the input source
- `val`: Pointer to int16 variable where the converted integer value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [CopyReadBinaryData](CopyReadBinaryData.md) (read raw binary data from input)
  - pg_ntoh16 (convert from network to host byte order)
- Called from (representative examples):
  - [NextCopyFrom](../N/NextCopyFrom.md) (src/backend/commands/copyfromparse.c:1023)

## Notes and Other Information
- Returns true on successful read, false on EOF or insufficient data
- Automatically handles byte order conversion for cross-platform compatibility
- Sets the output value to 0 on failure to suppress compiler warnings
- Used primarily for reading column counts and other 16-bit metadata in binary COPY format
- The function is declared as static inline for performance optimization
- Companion function to CopyGetInt32 for handling different integer sizes in binary format