# gss_read

## Location
src/interfaces/libpq/fe-secure-gssapi.c: 435 - 478

## Overview
A static wrapper function for pqsecure_raw_read that provides enhanced error handling and polling status reporting for GSSAPI connection establishment.

## Definition


## Detailed Description
This internal function wraps pqsecure_raw_read to provide standardized error handling and status reporting during GSSAPI transport negotiation. It converts low-level socket read results into PostgresPollingStatusType values that indicate whether the operation completed successfully, needs to retry reading, or failed permanently. The function includes special handling for EOF conditions by attempting a second read after checking socket readiness.

Key behaviors:
- Translates errno values into appropriate polling status codes
- Handles retryable errors (EAGAIN, EWOULDBLOCK, EINTR) vs permanent failures
- Implements EOF detection with retry logic using pqReadReady
- Returns detailed status information for non-blocking operation patterns

## Parameters / Member Variables
- : PostgreSQL connection object containing socket information
- : Buffer where received data will be stored
- : Maximum number of bytes to read into the buffer
- : Output parameter storing the actual number of bytes read or error code

## Dependencies
- Functions called/Symbols referenced:
  - pqsecure_raw_read (low-level socket read function)
  - pqReadReady (checks if data is available to read on socket)
- Called from:
  - pqsecure_open_gss (used during GSSAPI transport negotiation at multiple points)

## Notes and Other Information
- Static function scope - only used within fe-secure-gssapi.c
- Designed specifically for use during GSSAPI transport setup, not regular data transfer
- Implements the PostgreSQL polling pattern for non-blocking operations
- The double-read logic on EOF helps distinguish between temporary unavailability and true connection closure
- Return values: PGRES_POLLING_OK (success), PGRES_POLLING_READING (retry needed), PGRES_POLLING_FAILED (permanent error)