# lo_creat

## Location
src/interfaces/libpq/fe-lobj.c: 438 - 473

## Overview
Creates a new large object in the database and returns its Object ID (OID).

## Definition
```c
Oid lo_creat(PGconn *conn, int mode)
```

## Detailed Description
The lo_creat function creates a new large object in the PostgreSQL database. It is a legacy function that was originally designed to accept file permissions similar to the Unix creat() system call, but the mode parameter is now ignored by the backend. The function communicates with the backend via the fastpath interface to create the large object and returns its unique OID for subsequent operations.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle
- `mode`: File mode parameter (ignored by current implementation, kept for compatibility)

## Dependencies
- Functions called/Symbols referenced:
  - lo_initialize
  - PQfn
  - PQArgBlock
  - PGRES_COMMAND_OK
  - InvalidOid
- Called from (representative examples):
  - lo_import_internal
  - importFile (test examples)

## Notes and Other Information
- Returns the OID of the newly created large object, or InvalidOid on failure
- The mode parameter is ignored but maintained for API compatibility
- Part of PostgreSQL's client-side large object interface (libpq)
- Located in src/interfaces/libpq/fe-lobj.c:438-473
- Legacy function - newer code should prefer lo_create() which allows specifying the OID
- Automatically assigns a system-generated OID to the new large object
- The created large object must be opened with lo_open before reading or writing