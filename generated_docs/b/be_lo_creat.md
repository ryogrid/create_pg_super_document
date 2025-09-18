# be_lo_creat

## Location
[src/backend/libpq/be-fsstubs.c:249-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L249-L261)

## Overview
A PostgreSQL backend function that creates a new large object and returns its object identifier (OID).

## Definition
```c
Datum be_lo_creat(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the backend support for PostgreSQL's lo_creat() large object function. It creates a new large object with an automatically assigned OID by calling inv_create with InvalidOid, which instructs the system to generate a unique identifier. The function includes read-only transaction protection and sets up cleanup tracking for proper resource management.

## Parameters / Member Variables
- No explicit parameters (uses PostgreSQL's PG_FUNCTION_ARGS mechanism)
- Returns: OID of the newly created large object

## Dependencies
- Functions called/Symbols referenced:
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md) (prevents execution in read-only transactions)
  - [inv_create](../i/inv_create.md) (internal large object creation function)
  - PG_RETURN_OID (OID return value macro)
  - InvalidOid (constant for auto-generated OID assignment)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function call mechanism)

## Notes and Other Information
- Located in src/backend/libpq/be-fsstubs.c:249-261
- Sets lo_cleanup_needed = true for proper resource cleanup
- Uses InvalidOid to request automatic OID assignment
- Protected against execution in read-only transactions
- Part of PostgreSQL's large object API for creating binary data containers
- The created large object starts empty and can be written to using other lo_* functions