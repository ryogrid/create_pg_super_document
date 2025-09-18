# pqsecure_raw_write

## Location
src/interfaces/libpq/fe-secure.c: 331 - 442

## Overview
Performs low-level unencrypted socket-based writing to a PostgreSQL connection, implementing sophisticated error handling that defers write failures to prioritize server error messages.

## Definition
```c
ssize_t pqsecure_raw_write(PGconn *conn, const void *ptr, size_t len)
```

## Detailed Description
This function implements the lowest-level writing mechanism for PostgreSQL connections, using the standard `send()` system call to write data directly to the socket. It serves as both the direct writing method for unencrypted connections and the physical I/O backend for encrypted connections (SSL/GSS).

The function implements a unique error handling strategy designed to handle TCP stack race conditions:
- Retryable errors (EINTR, EAGAIN, EWOULDBLOCK) return negative values for immediate retry
- Hard failures (EPIPE, ECONNRESET, etc.) are stored in `conn->write_failed` and `conn->write_err_msg` but the function claims success (returns `len`)
- This deferred error reporting allows the connection layer to prioritize server-provided error messages over socket-level failures
- Once `write_failed` is set, all subsequent writes are silently discarded to maintain message boundary synchronization

The function also handles SIGPIPE prevention through MSG_NOSIGNAL flags and signal masking, with fallback mechanisms for systems that don't support MSG_NOSIGNAL.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection structure (PGconn)
- `ptr`: Pointer to the data to be written (const to indicate it won't be modified)
- `len`: Number of bytes to write

## Dependencies
- Functions called/Symbols referenced:
  - `send` (system call for socket writing)
  - `DECLARE_SIGPIPE_INFO`, `DISABLE_SIGPIPE`, `RESTORE_SIGPIPE` (SIGPIPE handling macros)
  - `REMEMBER_EPIPE` (EPIPE tracking macro)
  - `SOCK_ERRNO`, `SOCK_ERRNO_SET` (socket errno handling macros)
  - `SOCK_STRERROR` (socket error string formatting)
  - [libpq_gettext](../l/libpq_gettext.md) (internationalization support)
  - `strlcat`, `strdup` (string manipulation functions)
  - `PG_STRERROR_R_BUFLEN` (buffer size constant)
- Called from (representative examples):
  - [pqsecure_write](pqsecure_write.md) (in fe-secure.c:301)
  - [pg_GSS_write](pg_GSS_write.md) (in fe-secure-gssapi.c:154)
  - [my_sock_write](../m/my_sock_write.md) (in fe-secure-openssl.c:1946)

## Notes and Other Information
- Returns the number of bytes written on success, or -1 only for retryable errors
- For hard failures, returns `len` (claiming success) while storing the actual error internally
- Once a write failure occurs, all subsequent writes are discarded until connection reset
- Implements MSG_NOSIGNAL fallback logic for systems that don't support this flag
- The unique error handling strategy is specifically designed to work well with OpenSSL and other encryption layers
- Error messages are stored with internationalization support using `libpq_gettext`
- Used as the foundation for all PostgreSQL connection writing, whether encrypted or not