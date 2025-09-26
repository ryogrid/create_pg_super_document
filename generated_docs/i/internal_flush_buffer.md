# internal_flush_buffer

## Location
[src/backend/libpq/pqcomm.c:1359-1431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1359-L1431)

## Overview
The core low-level function that performs actual data transmission to PostgreSQL clients, handling partial writes, error conditions, and connection management.

## Definition

```c
static pg_noinline int
internal_flush_buffer(const char *buf, size_t *start, size_t *end)
```
## Detailed Description
The  function is the fundamental workhorse of PostgreSQL's client communication system. It handles the actual transmission of buffered data to client connections with comprehensive error handling and state management.

Key features include:
1. **Partial Write Handling**: Manages scenarios where not all data can be sent in a single system call
2. **Non-blocking Support**: Gracefully handles non-blocking socket operations
3. **Error Recovery**: Implements sophisticated error handling including duplicate error suppression
4. **Connection Management**: Properly manages connection state when transmission fails
5. **Interrupt Handling**: Continues operation when interrupted by signals

The function uses  for actual data transmission, which provides SSL/TLS encryption support when configured. It includes critical safety measures to prevent recursive errors and stack overflow during error reporting.

## Parameters / Member Variables
- : Pointer to the buffer containing data to be sent
- : Pointer to the starting offset within the buffer (updated as data is sent)
- : Pointer to the ending offset within the buffer (marks end of valid data)

## Dependencies
- Functions called/Symbols referenced:
  - [secure_write](../s/secure_write.md) (encrypted/secure data transmission)
  - ereport/COMMERROR (error reporting)
  - [errcode_for_socket_access](../e/errcode_for_socket_access.md) (socket error code mapping)
  - EINTR, EAGAIN, EWOULDBLOCK (system error constants)
- Called from (representative examples):
  - [internal_flush](internal_flush.md) (standard buffer flushing)
  - [internal_putbytes](internal_putbytes.md) (large data bypass flushing)

## Notes and Other Information
- Function is marked as pg_noinline to prevent inlining (likely for debugging and stack trace clarity)
- Uses static variable  to suppress duplicate error messages
- Sets  and  flags when transmission fails permanently
- Resets buffer pointers () after successful transmission or permanent failure
- Includes critical safety check to prevent recursive ereport() calls that could cause stack overflow
- Returns 0 on success or when operation would block in non-blocking mode
- Returns EOF on permanent transmission errors
- Core component of PostgreSQL's secure communication infrastructure