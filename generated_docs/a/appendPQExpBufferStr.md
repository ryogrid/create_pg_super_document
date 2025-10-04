# appendPQExpBufferStr

## Location
[src/interfaces/libpq/pqexpbuffer.c:367-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/pqexpbuffer.c#L367-L377)

## Overview
A utility function that appends a null-terminated string to a PQExpBuffer, automatically allocating additional space if necessary.

## Definition

```c
void
appendPQExpBufferStr(PQExpBuffer str, const char *data)
```
## Detailed Description
The appendPQExpBufferStr function provides a convenient way to append string data to a PQExpBuffer object. It serves as a wrapper around appendBinaryPQExpBuffer, internally calculating the string length using strlen() and delegating the actual buffer management and data copying to the binary append function. This function is part of PostgreSQL's libpq interface for building dynamic SQL queries and other string content.

The function automatically handles memory allocation, expanding the buffer as needed to accommodate the new string data. It's designed to be used for building complex SQL statements, connection strings, and other textual content in PostgreSQL client applications.

## Parameters / Member Variables
- `str`: A PQExpBuffer object that will receive the appended string data
- `data`: A null-terminated C string to be appended to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [appendBinaryPQExpBuffer](appendBinaryPQExpBuffer.md)
  - strlen (standard C library function)
- Called from (representative examples):
  - Used extensively throughout PostgreSQL client utilities including pg_dump, pg_basebackup, pg_ctl, and libpq itself
  - Common usage patterns include building SQL queries, connection strings, and command-line arguments

## Notes and Other Information
- This function is exported by libpq and available to external applications
- The function assumes the input data is a valid null-terminated string
- Memory allocation failures are handled by the underlying appendBinaryPQExpBuffer function
- Widely used across PostgreSQL utilities for string manipulation and query construction
- Located in src/interfaces/libpq/pqexpbuffer.c:367-377

## Simplified Source

```c
void
appendPQExpBufferStr(PQExpBuffer str, const char *data)
{
    // Simply append the string as binary data
    appendBinaryPQExpBuffer(str, data, strlen(data));
}
```