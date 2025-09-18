# appendPQExpBuffer

## Location
[src/interfaces/libpq/pqexpbuffer.c:265-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/pqexpbuffer.c#L265-L293)

## Overview
Formats text data using printf-style formatting and appends it to the existing contents of a PQExpBuffer, expanding the buffer as needed.

## Definition
```c
void appendPQExpBuffer(PQExpBuffer str, const char *fmt, ...)
```

## Detailed Description
The `appendPQExpBuffer` function provides printf-style formatted text appending to a PQExpBuffer. Unlike `printfPQExpBuffer` which replaces the buffer contents, this function appends the formatted text to whatever data is already present in the buffer.

The function operates as a combination of sprintf and strcat:
1. **State validation**: Checks if the buffer is already in a broken state and returns early if so
2. **Format and append**: Uses a retry loop with `appendPQExpBufferVA` to format and append the text
3. **Buffer management**: Automatically handles buffer enlargement through the underlying `appendPQExpBufferVA` function
4. **Error preservation**: Saves and restores errno to avoid interfering with error handling

The retry loop ensures that if the buffer needs to be enlarged during the formatting operation, the formatting will be retried with the larger buffer until it succeeds or fails definitively.

## Parameters / Member Variables
- `str`: Pointer to the PQExpBuffer structure to append to
- `fmt`: Printf-style format string
- `...`: Variable arguments corresponding to the format string

## Dependencies
- Functions called/Symbols referenced:
  - PQExpBufferBroken
  - [appendPQExpBufferVA](appendPQExpBufferVA.md)
- Called from (representative examples):
  - No direct references found in the current codebase analysis

## Notes and Other Information
- This is a variadic function that accepts variable arguments like printf
- Preserves existing buffer contents, only adding new formatted text
- Automatically handles memory management and buffer expansion
- Uses errno preservation to maintain proper error state
- Part of the core libpq expandable string buffer interface
- The function is void - errors are communicated through the buffer's broken state
- More commonly used indirectly through other buffer manipulation functions