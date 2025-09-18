# lo_initialize

## Location
src/interfaces/libpq/fe-lobj.c: 843 - 1022

## Overview
Static initialization function that discovers and caches function OIDs for all PostgreSQL large object operations, ensuring efficient subsequent large object function calls.

## Definition
```c
static int lo_initialize(PGconn *conn)
```

## Detailed Description
This static function initializes the large object function infrastructure for a PostgreSQL connection by querying the system catalog to discover the Object IDs (OIDs) of all large object-related functions. It performs this discovery only once per connection and caches the results in the connection structure for efficient reuse.

The function queries pg_catalog.pg_proc to find function OIDs for all required large object operations including lo_open, lo_close, lo_creat, lo_create, lo_unlink, lo_lseek, lo_lseek64, lo_tell, lo_tell64, lo_truncate, lo_truncate64, loread, and lowrite. It validates that all essential functions are available and reports specific errors for missing functions.

This initialization approach allows PostgreSQL to support large objects across different server versions by dynamically discovering available functionality rather than assuming fixed function OIDs.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle to initialize for large object operations

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - malloc
  - MemSet
  - PQexec
  - PQgetvalue
  - PQntuples
  - PQclear
  - libpq_append_conn_error
  - strcmp
  - atoi
- Called from (representative examples):
  - lo_open
  - lo_close
  - lo_truncate
  - lo_read
  - lo_write
  - lo_lseek
  - lo_creat
  - lo_tell
  - lo_unlink

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Static function, only accessible within fe-lobj.c
- Performs one-time initialization per connection, subsequent calls return immediately if already initialized
- Allocates and populates PGlobjfuncs structure to cache function OIDs
- Validates presence of all essential large object functions (stone age functions)
- Gracefully handles newer functions that may not exist in older PostgreSQL versions
- Essential for the client-side large object API to function correctly
- Memory allocation failure and missing critical functions result in initialization failure
- Clears connection error state at the beginning of operation