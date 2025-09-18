# PQescapeByteaConn

## Location
src/interfaces/libpq/fe-exec.c: 4514 - 4529

## Overview
PQescapeByteaConn is a connection-aware wrapper function that escapes binary data for safe inclusion in SQL bytea literals, automatically selecting the appropriate encoding format based on server capabilities.

## Definition


## Detailed Description
PQescapeByteaConn provides a connection-aware interface for bytea escaping that automatically determines the best encoding format based on the PostgreSQL server version and connection settings. It calls PQescapeByteaInternal with connection-specific parameters: uses the connection's standard_conforming_strings setting and automatically enables hexadecimal encoding for server versions 9.0 and later (which introduced more efficient hex format support). The function validates the connection handle, clears any previous error state, and delegates the actual escaping work to the internal implementation.

## Parameters / Member Variables
- : PostgreSQL connection handle containing server version and string settings
- : Source binary data to be escaped
- : Length of the source data in bytes
- : Pointer to store the length of the resulting escaped string

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - PQescapeByteaInternal
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