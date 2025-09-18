# lo_open

## Location
src/interfaces/libpq/fe-lobj.c: 57 - 95

## Overview
Opens an existing PostgreSQL large object and returns a file descriptor for subsequent large object operations.

## Definition


## Detailed Description
The  function provides access to an existing large object in PostgreSQL by opening it and returning a file descriptor that can be used for subsequent read, write, seek, and other large object operations. The function initializes the large object function lookup table if necessary, then calls the PostgreSQL backend function  through the function call interface (). The operation mode parameter determines what operations are permitted on the opened large object.

## Parameters / Member Variables
- : PostgreSQL connection handle for the database session
- : Object identifier (OID) of the large object to open  
- : Access mode flags determining permitted operations (read, write, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - lo_initialize
  - PQfn
  - PQresultStatus
  - PQclear
- Called from (representative examples):
  - StartRestoreLO
  - dumpLOs
  - lo_import_internal
  - lo_export
  - importFile
  - exportFile

## Notes and Other Information
- Returns -1 on failure, otherwise returns a valid file descriptor
- The returned file descriptor is used for subsequent large object operations
- Requires that the large object already exists in the database
- The function automatically initializes the large object function lookup table on first use
- Access mode determines what operations are permitted on the opened large object
- Used extensively in pg_dump utilities and test programs for large object manipulation