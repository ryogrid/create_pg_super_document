# PGconn

## Location
src/interfaces/libpq/libpq-fe.h: 186 - 190

## Overview
PGconn is the main opaque structure that encapsulates a connection to a PostgreSQL backend server. It stores all state data associated with a single database connection including connection parameters, authentication state, communication buffers, and query results.

## Definition


The actual structure definition is in  as .

## Detailed Description
PGconn represents a database connection handle in libpq, PostgreSQL's C client library. The structure is intentionally opaque to applications - clients interact with it through libpq API functions rather than accessing its members directly. This design provides encapsulation and allows the internal structure to evolve without breaking compatibility.

The structure maintains comprehensive connection state including:
- Connection configuration (host, port, database, user credentials)
- SSL/TLS and authentication state  
- Network socket information and communication buffers
- Pipeline and transaction status
- Query results and notification queues
- Error handling and debugging information

All libpq functions that communicate with PostgreSQL require a PGconn pointer as their first parameter.

## Parameters / Member Variables
Key categories of member variables in the underlying  struct:

- **Connection Configuration**: , , , ,  - Basic connection parameters
- **SSL Configuration**: , , ,  - SSL/TLS settings
- **Authentication**: , ,  - Authentication state
- **Network State**: , , ,  - Socket and protocol information  
- **Status Indicators**: , ,  - Connection and transaction state
- **Buffers**: , ,  - Communication and error buffers
- **Query State**: , ,  - Query execution state
- **Notifications**: ,  - Asynchronous notification queue

## Dependencies
- Functions called/Symbols referenced:
  - pg_conn (the underlying struct type)
- Called from (representative examples):
  - All libpq API functions that operate on connections
  - PQconnectdb, PQexec, PQfinish, etc.

## Notes and Other Information
- The contents of PGconn are deliberately hidden from applications to maintain API stability
- Each PGconn represents exactly one database connection
- Connections can be in various states (connecting, connected, failed, etc.)
- The structure supports both synchronous and asynchronous operation modes
- Memory management for PGconn is handled by libpq (allocated by PQconnectdb family, freed by PQfinish)
- Thread safety: PGconn objects are not thread-safe and should not be shared between threads without external synchronization