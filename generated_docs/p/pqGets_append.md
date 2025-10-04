# pqGets_append

## Location
[src/interfaces/libpq/fe-misc.c:142-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L142-L151)

## Overview
pqGets_append is a function that reads a null-terminated string from a PostgreSQL connection and appends it to an existing PQExpBuffer without resetting the buffer contents.

## Definition
```c
int pqGets_append(PQExpBuffer buf, PGconn *conn)
```

## Detailed Description
pqGets_append is a wrapper function that calls the internal pqGets_internal function with the resetbuffer parameter set to false. This means it appends the incoming string data to the existing contents of the buffer rather than replacing them. The function reads characters from the connection's input buffer until it encounters a null terminator, then copies that string segment into the provided PQExpBuffer.

The function handles memory management gracefully - if the buffer cannot be expanded to accommodate the new data, the excess characters are silently discarded but the function continues to read through the entire string to maintain proper buffer positioning.

## Parameters / Member Variables
- `buf`: PQExpBuffer to append the read string data to (existing contents preserved)
- `conn`: PGconn connection object containing the input buffer to read from

## Dependencies
- Functions called/Symbols referenced:
  - [pqGets_internal](pqGets_internal.md) (with resetbuffer=false)
- Called from (representative examples):
  - MAX_ERRLEN (referenced in fe-connect.c:3755)
  - pgunlock_thread (referenced in libpq-int.h:746)

## Notes and Other Information
- Returns 0 on success, EOF if no complete null-terminated string is available
- This is the "append" variant that preserves existing buffer contents, unlike the regular pqGets function
- Part of the libpq client library's internal string handling utilities
- The function maintains the connection's input cursor position after reading
- Memory allocation failures are handled gracefully by discarding excess data rather than failing

## Simplified Source

```c
int pqGets_append(PQExpBuffer buf, PGconn *conn) {
    // Append string from connection to buffer (preserve existing contents)
    return pqGets_internal(buf, conn, false);
}
```