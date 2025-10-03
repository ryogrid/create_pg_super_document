# pqGetInt

## Location
[src/interfaces/libpq/fe-misc.c:216-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L216-L252)

## Overview
Reads a 2 or 4 byte integer from the input buffer and converts it from network byte order to local byte order.

## Definition

```c
int
pqGetInt(int *result, size_t bytes, PGconn *conn)
```
## Detailed Description
The  function is a utility function used in the libpq library to read integer values from the input buffer of a PostgreSQL connection. It handles both 2-byte and 4-byte integers, automatically converting them from network byte order (big-endian) to the local machine's byte order. This is essential for proper interpretation of binary data received from the PostgreSQL server, which always sends data in network byte order regardless of the server's native byte order.

The function performs bounds checking to ensure sufficient data is available in the input buffer before attempting to read. If insufficient data is available, it returns EOF. The function advances the input cursor position after successfully reading the data.

## Parameters / Member Variables
- `*result`: Pointer to an integer where the converted value will be stored
- `bytes`: Number of bytes to read (must be 2 or 4)
- `*conn`: PostgreSQL connection object containing the input buffer and cursor position
## Dependencies
- Functions called/Symbols referenced:
  - pg_ntoh16 (converts 16-bit value from network to host byte order)
  - pg_ntoh32 (converts 32-bit value from network to host byte order)
  - [pqInternalNotice](pqInternalNotice.md) (logs internal notice messages)
- Called from (representative examples):
  - [pqParseInput3](pqParseInput3.md) (protocol parsing)
  - [getRowDescriptions](../g/getRowDescriptions.md) (result set metadata parsing)
  - [getAnotherTuple](../g/getAnotherTuple.md) (data row parsing)
  - [getNotify](../g/getNotify.md) (notification message parsing)
  - [pqFunctionCall3](pqFunctionCall3.md) (function call result parsing)

## Notes and Other Information
- Only supports 2-byte and 4-byte integers; other sizes will result in an error notice and EOF return
- Returns 0 on success, EOF on failure (insufficient data or unsupported size)
- The function uses memcpy to safely extract bytes from the buffer, avoiding potential alignment issues
- Critical for parsing PostgreSQL protocol messages which use network byte order for all integer fields
- Part of the libpq internal API, not exposed to client applications directly

## Simplified Source

```c
int pqGetInt(int *result, size_t bytes, PGconn *conn)
{
    uint16 tmp2;
    uint32 tmp4;

    switch (bytes) {
        case 2:
            // Check if enough data available
            if (conn->inCursor + 2 > conn->inEnd)
                return EOF;

            // Read 2 bytes and convert from network byte order
            memcpy(&tmp2, conn->inBuffer + conn->inCursor, 2);
            conn->inCursor += 2;
            *result = (int) pg_ntoh16(tmp2);
            break;

        case 4:
            // Check if enough data available
            if (conn->inCursor + 4 > conn->inEnd)
                return EOF;

            // Read 4 bytes and convert from network byte order
            memcpy(&tmp4, conn->inBuffer + conn->inCursor, 4);
            conn->inCursor += 4;
            *result = (int) pg_ntoh32(tmp4);
            break;

        default:
            // Unsupported integer size
            pqInternalNotice(&conn->noticeHooks,
                           "integer of size %lu not supported by pqGetInt",
                           (unsigned long) bytes);
            return EOF;
    }

    return 0;  // Success
}
```