# PQconnectdbParams

## Location
src/interfaces/libpq/fe-connect.c: 689 - 706

## Overview
Establishes a synchronous (blocking) connection to a PostgreSQL backend using connection parameters provided in two parallel arrays of keywords and values.

## Definition
```c
PGconn *PQconnectdbParams(const char *const *keywords, const char *const *values, int expand_dbname)
```

## Detailed Description
This function provides a synchronous interface for establishing PostgreSQL database connections using structured parameter arrays rather than connection strings. It internally uses PQconnectStartParams to initiate the connection and pqConnectDBComplete to complete the connection process in a blocking manner.

The function is part of libpq's preferred modern connection API, offering better extensibility compared to older functions like PQsetdb and PQsetdbLogin. It handles the complete connection establishment process including parameter validation, network connection, authentication, and initial server communication.

## Parameters / Member Variables
- `keywords`: NULL-terminated array of connection parameter names (e.g., "host", "port", "dbname")
- `values`: NULL-terminated array of corresponding parameter values, parallel to keywords array
- `expand_dbname`: Integer flag indicating whether to expand the database name if it appears to be a connection URI or connection string

## Dependencies
- Functions called/Symbols referenced:
  - PQconnectStartParams
  - pqConnectDBComplete
  - CONNECTION_BAD (constant)

- Called from (representative examples):
  - GetConnection (pg_basebackup)
  - ConnectDatabase (pg_dump)
  - connectDatabase (pg_dumpall, fe_utils)
  - ECPGconnect (ECPG interface)
  - copy_connection (libpq_pipeline test module)

## Notes and Other Information
- Returns a PGconn pointer that must be freed with PQfinish regardless of connection success
- Connection status should be checked using PQstatus() before using the connection
- Preferred over PQconnectdb when connection parameters are already available in structured form
- Part of the synchronous connection API - blocks until connection completes or fails
- Widely used throughout PostgreSQL client tools and utilities for reliable connection establishment
- The expand_dbname parameter allows for flexible handling of database names that may contain embedded connection information