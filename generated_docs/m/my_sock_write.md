# my_sock_write

## Location
[src/interfaces/libpq/fe-secure-openssl.c:1942-1971](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L1942-L1971)

## Overview
A BIO write callback function that wraps PostgreSQL's secure_raw_write for use with OpenSSL's BIO (Basic Input/Output) abstraction layer.

## Definition

```c
static int
my_sock_write(BIO *h, const char *buf, int size)
```
## Detailed Description
This function serves as a custom BIO write method for PostgreSQL's OpenSSL integration. It acts as an adapter between OpenSSL's BIO interface and PostgreSQL's internal secure socket writing functionality. The function handles write operations on SSL-enabled connections by calling the underlying secure_raw_write function and properly managing BIO retry flags for non-blocking I/O scenarios.

When a write operation fails due to interruption or would block, the function sets appropriate retry flags to inform the OpenSSL library that the operation should be retried later. This is essential for proper handling of non-blocking socket operations in PostgreSQL's event-driven architecture.

## Parameters / Member Variables
- : BIO structure pointer containing connection state and app data
- : Buffer containing data to write
- : Number of bytes to write

## Dependencies
- Functions called/Symbols referenced:
  - [secure_raw_write](../s/secure_raw_write.md)
  - BIO_get_app_data
  - BIO_clear_retry_flags
  - BIO_set_retry_write
- Called from (representative examples):
  - [my_BIO_s_socket](my_BIO_s_socket.md) (in BIO method structure initialization)

## Notes and Other Information
- This is a static function used internally within the OpenSSL security module
- Handles EINTR, EWOULDBLOCK, and EAGAIN errno values to support non-blocking I/O
- Returns the number of bytes written, or <= 0 on error
- The function extracts the Port structure from BIO app data to access PostgreSQL's connection context
- Part of PostgreSQL's custom BIO implementation for secure socket communication
- Companion function to my_sock_read for bidirectional SSL communication