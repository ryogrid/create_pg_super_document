# PQputline

## Location
[src/interfaces/libpq/fe-exec.c:2918-2927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2918-L2927)

## Overview
PQputline is a deprecated PostgreSQL libpq function that sends a string to the backend during COPY IN operations, providing a simple wrapper around PQputnbytes for backwards compatibility.

## Definition

```c
int
PQputline(PGconn *conn, const char *string)
```
## Detailed Description
PQputline is a legacy function designed to send string data to the PostgreSQL backend during COPY IN operations. The function is marked as deprecated primarily because its return convention doesn't allow the caller to distinguish between a hard error and a send failure in non-blocking mode. It internally calls PQputnbytes with the length of the string calculated using strlen(), making it essentially a convenience wrapper for null-terminated strings.

The function returns 0 if the operation is successful and EOF if it fails. However, this binary return value is insufficient for proper error handling in modern applications, especially those using non-blocking connections.

## Parameters / Member Variables
- `*conn`: Connection object representing the database connection
- `*string`: Null-terminated string to be sent to the backend during COPY IN
## Dependencies
- Functions called/Symbols referenced:
  - [PQputnbytes](PQputnbytes.md)
  - strlen (standard C library function)
- Called from (representative examples):
  - [initPopulateTable](../i/initPopulateTable.md) (in pgbench)

## Notes and Other Information
- This function is deprecated and should be avoided in new code
- Use PQputnbytes instead for better error handling capabilities
- The deprecation is due to inadequate return value semantics that don't provide sufficient error information
- Located in src/interfaces/libpq/fe-exec.c:2918-2927
- Part of the PostgreSQL COPY protocol implementation in libpq

## Simplified Source

```c
int PQputline(PGconn *conn, const char *string) {
    // Simple wrapper: send the string with its length calculated by strlen
    return PQputnbytes(conn, string, strlen(string));
}
```