# lo_create

## Location
[src/interfaces/libpq/fe-lobj.c:474-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L474-L514)

## Overview
Creates a new large object in the database, optionally with a specific OID, and returns the Object ID.

## Definition
```c
Oid lo_create(PGconn *conn, Oid lobjId)
```

## Detailed Description
The lo_create function creates a new large object in the PostgreSQL database with enhanced control compared to lo_creat. It allows the caller to specify a desired OID for the large object, or pass InvalidOid to let the system assign one automatically. This function is preferred over lo_creat for new code as it provides more flexibility. The function requires PostgreSQL 8.1 or later and verifies backend support before attempting the operation.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle
- `lobjId`: Desired OID for the new large object, or InvalidOid for system assignment

## Dependencies
- Functions called/Symbols referenced:
  - [lo_initialize](lo_initialize.md)
  - [PQfn](../P/PQfn.md)
  - PQArgBlock
  - PGRES_COMMAND_OK
  - InvalidOid
- Called from (representative examples):
  - [StartRestoreLO](../S/StartRestoreLO.md)
  - [lo_import_internal](lo_import_internal.md)

## Notes and Other Information
- Returns the OID of the newly created large object, or InvalidOid on failure
- Allows specifying a specific OID (useful for pg_dump/restore operations)
- Part of PostgreSQL's client-side large object interface (libpq)
- Located in src/interfaces/libpq/fe-lobj.c:474-514
- Requires PostgreSQL 8.1+ (checks fn_lo_create availability)
- Preferred over lo_creat() for new applications
- If specified OID already exists, the function will fail
- The created large object must be opened with lo_open before reading or writing