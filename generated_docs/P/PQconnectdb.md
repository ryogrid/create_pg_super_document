# PQconnectdb

## Location
src/interfaces/libpq/fe-connect.c: 744 - 759

## Overview
Establishes a synchronous connection to a PostgreSQL backend through the postmaster using connection information specified in a connection string.

## Definition
```c
PGconn *PQconnectdb(const char *conninfo)
```

## Detailed Description
PQconnectdb is a high-level convenience function that creates a complete database connection in a single blocking call. It internally uses PQconnectStart to initiate the connection and then calls pqConnectDBComplete to finish the connection establishment process synchronously. The function accepts a connection string that can be either a whitespace-separated list of "option = value" pairs or a URI format. If the connection process fails at any point, the returned PGconn structure will have its status field set to CONNECTION_BAD, but the structure itself will still be valid and should be freed with PQfinish.

## Parameters / Member Variables
- `conninfo`: A connection string containing database connection parameters, either as space-separated key=value pairs or as a PostgreSQL URI

## Dependencies
- Functions called/Symbols referenced:
  - [PQconnectStart](PQconnectStart.md)
  - [pqConnectDBComplete](../p/pqConnectDBComplete.md)
  - CONNECTION_BAD (connection status constant)
- Called from (representative examples):
  - connect_database (pg_createsubscriber.c)
  - [main](../m/main.md) (pg_rewind.c)
  - [get_db_conn](../g/get_db_conn.md) (pg_upgrade server.c)
  - Various test programs and examples

## Notes and Other Information
- Returns a PGconn pointer that must be freed with PQfinish regardless of connection success
- This is a blocking/synchronous operation - use PQconnectStart for asynchronous connections
- Even if the function returns a non-NULL PGconn, always check the connection status before use
- The connection string format supports both traditional parameter lists and URI notation
- Single quotes in parameter values must be escaped with backslashes
- Memory allocation failure is the only case where NULL is returned