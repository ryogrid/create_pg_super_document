# pqConnectDBStart

## Location
src/interfaces/libpq/fe-connect.c: 2392 - 2469

## Overview
Initiates the asynchronous connection process to a PostgreSQL backend server, setting up initial state and beginning the connection sequence.

## Definition


## Detailed Description
This function serves as the entry point for establishing a connection to a PostgreSQL backend. It performs essential initialization and validation steps before beginning the actual connection process. The function validates the connection object, ensures proper linking to frontend libraries, initializes connection buffers, and sets up the connection state machine for asynchronous operation.

Key responsibilities include verifying that libpq is correctly linked to frontend functions (not backend internals), clearing input/output buffers, configuring host selection parameters for connection attempts, setting the initial connection status, and invoking the connection polling mechanism. The function is designed to work with PostgreSQL's asynchronous connection model, where the actual connection process continues through subsequent calls to PQconnectPoll.

Special handling is provided for cancel requests, which should only attempt connection to a single host and address. The function also manages server type preferences, resetting the target server type from PASS2 to the initial state if needed for reconnection scenarios.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the PostgreSQL connection. The function modifies various fields including connection status, buffer positions, host selection state, and error messages.

## Dependencies
- Functions called/Symbols referenced:
  - pg_link_canary_is_frontend (validates proper library linking)
  - appendPQExpBufferStr (appends error messages to connection)
  - PQconnectPoll (continues the asynchronous connection process)
  - pqDropConnection (closes connection on failure)
  - CONNECTION_NEEDED, CONNECTION_BAD (connection status constants)
  - SERVER_TYPE_PREFER_STANDBY_PASS2, SERVER_TYPE_PREFER_STANDBY (server type constants)
  - PGRES_POLLING_WRITING (polling status constant)
- Called from (representative examples):
  - PQcancelStart (connection cancellation)
  - PQconnectStartParams (parameterized connection start)
  - PQconnectStart (standard connection start)
  - PQsetdbLogin (legacy connection interface)
  - PQreset (connection reset)
  - PQresetStart (asynchronous connection reset)

## Notes and Other Information
- Returns 1 on successful initialization leading to PGRES_POLLING_WRITING state, 0 on failure
- Designed for asynchronous operation, requires subsequent PQconnectPoll calls to complete connection
- Includes developer-oriented link validation to detect improper library linking
- Handles both normal connections and cancel request connections with different host selection logic
- Automatically closes any opened socket on failure to prevent resource leaks
- Part of PostgreSQL's robust connection establishment and recovery system
- Sets connection status to CONNECTION_BAD on failure, allowing proper error reporting to applications