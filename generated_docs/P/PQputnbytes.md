# PQputnbytes

## Location
[src/interfaces/libpq/fe-exec.c:2928-2948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2928-L2948)

## Overview
PQputnbytes is a PostgreSQL libpq function that sends a specified number of bytes to the backend during COPY IN operations, providing an alternative to PQputline that doesn't require null-terminated strings.

## Definition
```c
int PQputnbytes(PGconn *conn, const char *buffer, int nbytes)
```

## Detailed Description
PQputnbytes is similar to PQputline but provides more flexibility by accepting a buffer that doesn't need to be null-terminated, along with an explicit byte count. This function is useful when sending binary data or when the exact length of data to send is known in advance. It serves as a wrapper around PQputCopyData, simplifying the return value to a binary success/failure indication.

The function returns 0 if the operation is successful and EOF if it fails. Like PQputline, this function has limited error reporting capabilities compared to the more modern PQputCopyData function it wraps.

## Parameters / Member Variables
- `conn`: Connection object representing the database connection
- `buffer`: Pointer to the data buffer to be sent (need not be null-terminated)
- `nbytes`: Number of bytes to send from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [PQputCopyData](PQputCopyData.md)
- Called from (representative examples):
  - [PQputline](PQputline.md)

## Notes and Other Information
- Provides more control than PQputline by accepting explicit byte counts
- Does not require null-terminated strings, making it suitable for binary data
- Acts as a compatibility wrapper around the more modern PQputCopyData function
- Returns simplified error codes (0 for success, EOF for failure)
- Located in src/interfaces/libpq/fe-exec.c:2928-2948
- Part of the PostgreSQL COPY protocol implementation in libpq