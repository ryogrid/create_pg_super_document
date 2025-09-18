# pqsecure_raw_read

## Location
src/interfaces/libpq/fe-secure.c: 208 - 281

## Overview
Performs unencrypted socket-based reading from a PostgreSQL connection, handling various error conditions and providing appropriate error messages.

## Definition
```c
ssize_t pqsecure_raw_read(PGconn *conn, void *ptr, size_t len)
```

## Detailed Description
This function implements the lowest-level reading mechanism for PostgreSQL connections, using the standard `recv()` system call to read data directly from the socket. It provides comprehensive error handling for various network conditions:

- Handles retryable conditions (EAGAIN, EWOULDBLOCK, EINTR) without setting error messages
- Detects connection closures (EPIPE, ECONNRESET) and provides informative error messages
- Manages errno state carefully to preserve error information for callers
- Converts certain error conditions (errno == 0) to EOF indicators

The function is used as the foundation for both encrypted reading functions (which may fall back to raw reading) and as the direct reading method for unencrypted connections.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection structure (PGconn)
- `ptr`: Buffer to store the read data
- `len`: Maximum number of bytes to read

## Dependencies
- Functions called/Symbols referenced:
  - `recv` (system call for socket reading)
  - `SOCK_ERRNO_SET` (macro for setting socket errno)
  - `SOCK_ERRNO` (macro for getting socket errno)
  - `SOCK_STRERROR` (macro for socket error strings)
  - `libpq_append_conn_error` (for error message formatting)
  - `PG_STRERROR_R_BUFLEN` (buffer size constant)
- Called from (representative examples):
  - `pqsecure_read` (in fe-secure.c:201)
  - `pg_GSS_read` (in fe-secure-gssapi.c:332, 365)
  - `my_sock_read` (in fe-secure-openssl.c:1913)

## Notes and Other Information
- Returns the number of bytes read on success, 0 on EOF, or -1 on error
- Carefully preserves and restores errno state to ensure proper error propagation to callers
- Distinguishes between retryable errors (no error message) and permanent failures (detailed error messages)
- Used as the foundation layer for both GSS and SSL reading implementations
- The function handles platform-specific socket error codes and provides unified error handling