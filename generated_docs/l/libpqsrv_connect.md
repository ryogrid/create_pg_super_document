# libpqsrv_connect

## Location
src/include/libpq/libpq-be-fe-helpers.h: 66 - 83

## Overview
A convenience wrapper around PQconnectdb() that handles file descriptor reservation and processes interrupts during connection establishment to PostgreSQL databases.

## Definition


## Detailed Description
libpqsrv_connect provides a server-side wrapper for PostgreSQL connection establishment that addresses resource management concerns specific to server processes. It reserves a file descriptor before attempting connection and ensures that interrupts are properly processed during the connection establishment phase. The function follows a prepare-connect-finalize pattern by first calling libpqsrv_connect_prepare(), then using PQconnectStart() for asynchronous connection initiation, and finally calling libpqsrv_connect_internal() to complete the connection process with interrupt handling.

The function will throw an error if file descriptor acquisition fails through AcquireExternalFD(), but does not throw errors for connection establishment failures themselves. This design allows callers to distinguish between resource exhaustion issues and network/authentication problems.

## Parameters / Member Variables
- `conninfo`: Connection string in PostgreSQL format containing database connection parameters
- `wait_event_info`: Event identifier used for wait event reporting during connection establishment

## Dependencies
- Functions called/Symbols referenced:
  - libpqsrv_connect_prepare
  - PQconnectStart
  - libpqsrv_connect_internal
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This is a static inline function defined in src/include/libpq/libpq-be-fe-helpers.h:66-83
- Callers must use PQstatus() to verify if the returned connection is valid since connection failures do not result in thrown errors
- Part of the libpqsrv suite of functions designed for server-side PostgreSQL connection management
- The function handles asynchronous connection establishment which allows for proper interrupt processing during potentially long connection setup phases