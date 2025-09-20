# pgtls_read_pending

## Location
[src/interfaces/libpq/fe-secure-openssl.c:256-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L256-L261)

## Overview
Checks whether there is pending encrypted data buffered in the SSL connection that can be read without blocking.

## Definition

```c
bool
pgtls_read_pending(PGconn *conn)
```
## Detailed Description
This function provides a simple wrapper around OpenSSL's SSL_pending() function to determine if there is any data that has already been read from the network and buffered within the SSL layer but not yet returned to the application. This is crucial for non-blocking I/O operations where the application needs to know if data is immediately available without performing a potentially blocking read operation.

The function is particularly important in SSL/TLS connections because the SSL layer may have read and decrypted more data from the network than was requested by the last read operation, keeping the extra data in internal buffers. This buffered data won't be detected by standard socket polling mechanisms (like select() or poll()), but can be immediately read without blocking.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object containing the SSL connection state

## Dependencies
- Functions called/Symbols referenced:
  - SSL_pending (OpenSSL function)
- Called from (representative examples):
  - [pqSocketCheck](pqSocketCheck.md) (in fe-misc.c:1081)
  - pgunlock_thread (referenced in libpq-int.h:824)

## Notes and Other Information
- Returns true if there is pending data that can be read immediately, false otherwise
- Essential for correct non-blocking I/O behavior with SSL connections
- The presence of pending data means a subsequent pgtls_read() call will not block
- This function should be checked before relying on socket-level readiness indicators
- Does not perform any I/O operations itself, only queries internal SSL buffer state
- Part of the secure connection interface for PostgreSQL's libpq client library
- Location: src/interfaces/libpq/fe-secure-openssl.c:256-261