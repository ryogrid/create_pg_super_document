# pqSocketCheck

## Location
[src/interfaces/libpq/fe-misc.c:1067-1116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1067-L1116)

## Overview
pqSocketCheck is a static PostgreSQL libpq function that performs the core socket monitoring using poll or select to check for read/write readiness with timeout support.

## Definition


## Detailed Description
pqSocketCheck is the foundational socket monitoring function used by other libpq waiting functions. It uses the system's PQsocketPoll function (which abstracts poll/select) to check if a socket is ready for reading, writing, or both operations. The function includes special handling for SSL connections by checking SSL library buffers before polling the socket directly. It also implements retry logic for interrupted system calls (EINTR) and provides comprehensive error handling with detailed error messages.

## Parameters / Member Variables
- : Pointer to the PGconn connection structure representing the database connection
- : Integer flag indicating whether to check for read readiness (non-zero means check for read)
- : Integer flag indicating whether to check for write readiness (non-zero means check for write)
- : Timeout specified as microseconds since Unix epoch (pg_usec_time_t). Use -1 for infinite timeout, 0 for immediate return

## Dependencies
- Functions called/Symbols referenced:
  - [PQsocketPoll](../P/PQsocketPoll.md)
  - [pgtls_read_pending](pgtls_read_pending.md) (SSL only)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - PGINVALID_SOCKET
  - SOCK_ERRNO
  - SOCK_STRERROR
  - EINTR
- Called from (representative examples):
  - [pqWaitTimed](pqWaitTimed.md)
  - [pqReadReady](pqReadReady.md)
  - [pqWriteReady](pqWriteReady.md)

## Notes and Other Information
- Returns >0 if one or more conditions are met, 0 if timeout occurred, -1 if error occurred
- For SSL connections, checks SSL library buffers first before polling the socket for read operations
- Implements retry logic for EINTR (interrupted system call) conditions
- Validates connection and socket before proceeding with the poll operation
- Static function - only used internally within fe-misc.c
- File location: src/interfaces/libpq/fe-misc.c:1067-1116