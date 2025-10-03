# pqGets_internal

## Location
[src/interfaces/libpq/fe-misc.c:109-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L109-L135)

## Overview
Internal function that reads a null-terminated string from the connection's input buffer and stores it in an expandable buffer.

## Definition

```c
static int
pqGets_internal(PQExpBuffer buf, PGconn *conn, bool resetbuffer)
```
## Detailed Description
pqGets_internal is a low-level utility function used internally by libpq to extract null-terminated strings from the connection's input buffer. It efficiently searches for a null terminator (\0) in the buffered data and copies the string into a PQExpBuffer for further processing.

The function operates by scanning through the connection's inBuffer starting from the current inCursor position until it finds a null terminator or reaches the end of available data (inEnd). If a complete null-terminated string is found, it copies the string data (excluding the terminator) to the provided buffer and advances the cursor past the null terminator.

This function is used as a building block for higher-level string reading functions and is critical for parsing protocol messages that contain null-terminated strings, such as error messages, field names, and various textual data.

## Parameters / Member Variables
- `buf`: PQExpBuffer where the extracted string will be stored
- `*conn`: Pointer to the PGconn structure representing the database connection
- `resetbuffer`: Boolean flag indicating whether to reset the buffer before appending the string
## Dependencies
- Functions called/Symbols referenced:
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md) (clears the buffer if resetbuffer is true)
  - [appendBinaryPQExpBuffer](../a/appendBinaryPQExpBuffer.md) (appends the string data to the buffer)
  - conn->inBuffer (connection input buffer)
  - conn->inCursor (current read position)
  - conn->inEnd (end of available data)
- Called from (representative examples):
  - [pqGets](pqGets.md) (public wrapper that resets the buffer)
  - [pqGets_append](pqGets_append.md) (public wrapper that appends to existing buffer content)

## Notes and Other Information
- Returns 0 on success, EOF when a complete null-terminated string is not available
- This is a static internal function, not exposed in the public API
- Efficiently handles string extraction by copying local variables for faster loop operations
- The function advances the connection's inCursor past the null terminator
- If resetbuffer is true, any existing content in the buffer is cleared before adding the new string
- Part of the protocol message parsing infrastructure
- Memory allocation for the buffer is handled by the PQExpBuffer functions

## Simplified Source

```c
static int
pqGets_internal(PQExpBuffer buf, PGconn *conn, bool resetbuffer)
{
    // Copy connection buffer info for faster search
    char *inBuffer = conn->inBuffer;
    int inCursor = conn->inCursor;
    int inEnd = conn->inEnd;

    // Find null terminator in buffer
    while (inCursor < inEnd && inBuffer[inCursor])
        inCursor++;

    // Check if complete string found
    if (inCursor >= inEnd)
        return EOF;

    // Calculate string length and copy to buffer
    int slen = inCursor - conn->inCursor;

    if (resetbuffer)
        resetPQExpBuffer(buf);

    appendBinaryPQExpBuffer(buf, inBuffer + conn->inCursor, slen);

    // Move cursor past the null terminator
    conn->inCursor = ++inCursor;

    return 0;
}
```