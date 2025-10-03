# GetCopyDataByte

## Location
[src/bin/pg_basebackup/pg_basebackup.c:1516-1529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L1516-L1529)

## Overview
Extracts a single byte from a COPY data message buffer and advances the cursor position.

## Definition

```c
static char
GetCopyDataByte(size_t r, char *copybuf, size_t *cursor)
```
## Detailed Description
GetCopyDataByte is a utility function that safely reads a single byte from a COPY protocol message buffer. It performs bounds checking to ensure that there are remaining bytes to read in the buffer before extracting the byte. The function automatically advances the cursor position to point to the next unread byte, making it suitable for sequential reading of message contents.

This function is typically used for reading message type indicators and other single-byte values from COPY protocol messages during base backup operations.

## Parameters / Member Variables
- `r`: Total size of the data buffer in bytes
- `*copybuf`: Pointer to the buffer containing the COPY data message
- `*cursor`: Pointer to the current position within the buffer, updated after reading
## Dependencies
- Functions called/Symbols referenced:
  - [ReportCopyDataParseError](../R/ReportCopyDataParseError.md)
- Called from (representative examples):
  - [ReceiveArchiveStreamChunk](../R/ReceiveArchiveStreamChunk.md)
  - CompressionLocation

## Notes and Other Information
- This function provides bounds checking to prevent buffer overrun when reading COPY data
- The cursor is automatically incremented after successful byte extraction
- If no bytes remain in the buffer, the function calls ReportCopyDataParseError and does not return
- This is a static utility function only used within pg_basebackup.c
- The function follows the pattern of safe sequential buffer reading used throughout the COPY protocol parsing code