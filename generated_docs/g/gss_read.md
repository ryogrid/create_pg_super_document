# gss_read

## Location
[src/interfaces/libpq/fe-secure-gssapi.c:435-478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-gssapi.c#L435-L478)

## Overview
A static wrapper function for pqsecure_raw_read that provides enhanced error handling and polling status reporting for GSSAPI connection establishment.

## Definition

```c
static PostgresPollingStatusType
gss_read(PGconn *conn, void *recv_buffer, size_t length, ssize_t *ret)
```
## Detailed Description
This internal function wraps pqsecure_raw_read to provide standardized error handling and status reporting during GSSAPI transport negotiation. It converts low-level socket read results into PostgresPollingStatusType values that indicate whether the operation completed successfully, needs to retry reading, or failed permanently. The function includes special handling for EOF conditions by attempting a second read after checking socket readiness.

Key behaviors:
- Translates errno values into appropriate polling status codes
- Handles retryable errors (EAGAIN, EWOULDBLOCK, EINTR) vs permanent failures
- Implements EOF detection with retry logic using pqReadReady
- Returns detailed status information for non-blocking operation patterns

## Parameters / Member Variables
- `*conn`: PostgreSQL connection object containing socket information
- `*recv_buffer`: Buffer where received data will be stored
- `length`: Maximum number of bytes to read into the buffer
- `*ret`: Output parameter storing the actual number of bytes read or error code
## Dependencies
- Functions called/Symbols referenced:
  - [pqsecure_raw_read](../p/pqsecure_raw_read.md) (low-level socket read function)
  - [pqReadReady](../p/pqReadReady.md) (checks if data is available to read on socket)
- Called from:
  - [pqsecure_open_gss](../p/pqsecure_open_gss.md) (used during GSSAPI transport negotiation at multiple points)

## Notes and Other Information
- Static function scope - only used within fe-secure-gssapi.c
- Designed specifically for use during GSSAPI transport setup, not regular data transfer
- Implements the PostgreSQL polling pattern for non-blocking operations
- The double-read logic on EOF helps distinguish between temporary unavailability and true connection closure
- Return values: PGRES_POLLING_OK (success), PGRES_POLLING_READING (retry needed), PGRES_POLLING_FAILED (permanent error)

## Simplified Source
```c
static PostgresPollingStatusType gss_read(PGconn *conn, void *recv_buffer,
                                          size_t length, ssize_t *ret) {
    // Attempt to read data from the socket
    *ret = pqsecure_raw_read(conn, recv_buffer, length);

    if (*ret < 0) {
        // Handle retryable errors vs permanent failures
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            return PGRES_POLLING_READING;
        else
            return PGRES_POLLING_FAILED;
    }

    // Handle EOF condition with retry logic
    if (*ret == 0) {
        // Check if data is actually available to read
        int result = pqReadReady(conn);
        if (result < 0)
            return PGRES_POLLING_FAILED;
        if (!result)
            return PGRES_POLLING_READING;

        // Try reading again after confirming readiness
        *ret = pqsecure_raw_read(conn, recv_buffer, length);
        if (*ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                return PGRES_POLLING_READING;
            else
                return PGRES_POLLING_FAILED;
        }

        // Still EOF after second attempt = connection closed
        if (*ret == 0)
            return PGRES_POLLING_FAILED;
    }

    return PGRES_POLLING_OK;
}
```