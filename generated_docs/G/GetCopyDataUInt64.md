# GetCopyDataUInt64

## Location
src/bin/pg_basebackup/pg_basebackup.c: 1554 - 1568

## Overview
Extracts an unsigned 64-bit integer from a COPY data message buffer and advances the cursor past the integer value.

## Definition


## Detailed Description
GetCopyDataUInt64 is a utility function that safely reads an 8-byte unsigned integer from a COPY protocol message buffer. It performs bounds checking to ensure that at least 8 bytes remain in the buffer before attempting to read the integer. The function uses memcpy to safely extract the bytes and applies network-to-host byte order conversion using pg_ntoh64 to handle endianness differences between client and server architectures.

This function is primarily used for reading numeric data fields in COPY protocol messages, such as progress report byte counts during base backup operations.

## Parameters / Member Variables
- : Total size of the data buffer in bytes
- : Pointer to the buffer containing the COPY data message
- : Pointer to the current position within the buffer, updated by 8 bytes after reading

## Dependencies
- Functions called/Symbols referenced:
  - ReportCopyDataParseError
  - pg_ntoh64
  - memcpy (standard library function)
- Called from (representative examples):
  - ReceiveArchiveStreamChunk
  - CompressionLocation

## Notes and Other Information
- The function performs bounds checking to ensure at least 8 bytes are available before reading
- Uses memcpy for safe memory access, avoiding potential alignment issues
- Applies network-to-host byte order conversion to handle endianness correctly
- The cursor is automatically advanced by 8 bytes (sizeof(uint64)) after successful reading
- If insufficient bytes remain in the buffer, the function calls ReportCopyDataParseError and does not return
- This is a static utility function only used within pg_basebackup.c for COPY protocol parsing
- The function follows PostgreSQL's conventions for handling binary data in network protocols