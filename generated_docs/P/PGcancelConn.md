# PGcancelConn

## Location
[src/interfaces/libpq/libpq-fe.h:191-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-fe.h#L191-L197)

## Overview
PGcancelConn is an opaque structure that encapsulates a cancel connection to a PostgreSQL backend. It provides a type-safe wrapper around PGconn specifically for sending query cancellation requests.

## Definition


The actual structure definition is in  as .

## Detailed Description
PGcancelConn is a specialized connection handle designed specifically for sending cancellation requests to PostgreSQL backends. It serves as a type-safe wrapper around the regular PGconn structure, ensuring that cancel-specific functions cannot accidentally be called with regular connection objects and vice versa.

The structure was introduced to provide a cleaner API for the newer asynchronous cancellation functions (PQcancelBlocking, PQcancelStart, PQcancelPoll) while maintaining type safety. Unlike the older PGcancel approach which required extracting cancellation information from an existing connection, PGcancelConn represents a dedicated connection object for cancellation purposes.

Key characteristics:
- Contains a single PGconn member for the actual connection functionality
- Provides compile-time type safety to prevent mixing regular and cancel connections  
- Supports both blocking and non-blocking cancellation operations
- Can be created independently or derived from existing connections

## Parameters / Member Variables
- **conn**:  - The underlying connection object that handles the actual network communication and protocol operations for sending cancellation requests

## Dependencies
- Functions called/Symbols referenced:
  - [PGconn](PGconn.md) (underlying connection type)
  - [pg_cancel_conn](../p/pg_cancel_conn.md) (the backing struct)
- Called from (representative examples):
  - [PQcancelCreate](PQcancelCreate.md) - Creates new cancel connection objects
  - [PQcancelBlocking](PQcancelBlocking.md) - Performs blocking cancellation
  - [PQcancelStart](PQcancelStart.md) - Initiates asynchronous cancellation
  - [PQcancelPoll](PQcancelPoll.md) - Polls asynchronous cancellation status
  - [PQcancelFinish](PQcancelFinish.md) - Cleans up cancel connection

## Notes and Other Information
- The structure contents are intentionally opaque to applications
- Provides type safety by preventing regular PGconn from being passed to cancel functions
- Can be created either from scratch or derived from existing PGconn objects
- Supports both synchronous (blocking) and asynchronous (non-blocking) cancellation
- Must be cleaned up with PQcancelFinish when no longer needed
- Introduced as part of the modern libpq cancellation API alongside the traditional PGcancel approach