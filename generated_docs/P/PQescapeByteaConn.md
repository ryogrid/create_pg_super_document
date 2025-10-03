# PQescapeByteaConn

## Location
[src/interfaces/libpq/fe-exec.c:4514-4529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L4514-L4529)

## Overview
PQescapeByteaConn is a connection-aware wrapper function that escapes binary data for safe inclusion in SQL bytea literals, automatically selecting the appropriate encoding format based on server capabilities.

## Definition

```c
unsigned char *
PQescapeByteaConn(PGconn *conn,
				  const unsigned char *from, size_t from_length,
				  size_t *to_length)
```
## Detailed Description
PQescapeByteaConn provides a connection-aware interface for bytea escaping that automatically determines the best encoding format based on the PostgreSQL server version and connection settings. It calls PQescapeByteaInternal with connection-specific parameters: uses the connection's standard_conforming_strings setting and automatically enables hexadecimal encoding for server versions 9.0 and later (which introduced more efficient hex format support). The function validates the connection handle, clears any previous error state, and delegates the actual escaping work to the internal implementation.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle containing server version and string settings
- `*from`: Source binary data to be escaped
- `from_length`: Length of the source data in bytes
- `*to_length`: Pointer to store the length of the resulting escaped string
## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - [PQescapeByteaInternal](PQescapeByteaInternal.md)
- Called from (representative examples):
  - Various libpq client applications (referenced in libpq-fe.h)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Returns NULL if the connection handle is invalid
- Automatically chooses hex format for PostgreSQL 9.0+ servers for better efficiency
- Uses connection's standard_conforming_strings setting to handle backslash escaping properly
- Clears connection error state before processing to ensure clean error reporting
- Part of the public libpq API for safe bytea handling in client applications
- Preferred over PQescapeBytea when a connection context is available for optimal format selection