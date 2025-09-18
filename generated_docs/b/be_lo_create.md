# be_lo_create

## Location
src/backend/libpq/be-fsstubs.c: 262 - 274

## Overview
A PostgreSQL backend function that creates a new large object with a specific OID or assigns a new OID if the provided one is invalid.

## Definition
```c
Datum be_lo_create(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the backend support for PostgreSQL's lo_create() large object function. Unlike be_lo_creat which always creates a large object with an automatically assigned OID, this function allows the caller to specify a desired OID. If the specified OID is available, it will be used; otherwise, the system will assign a new unique OID. The function includes read-only transaction protection and sets up cleanup tracking for proper resource management.

## Parameters / Member Variables
- `lobjId` (Oid): Desired OID for the large object (extracted from PG_GETARG_OID(0))
- Returns: OID of the created large object (may differ from input if requested OID was unavailable)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (OID parameter extraction macro)
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md) (prevents execution in read-only transactions)
  - [inv_create](../i/inv_create.md) (internal large object creation function)
  - PG_RETURN_OID (OID return value macro)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function call mechanism)

## Notes and Other Information
- Located in src/backend/libpq/be-fsstubs.c:262-274
- Sets lo_cleanup_needed = true for proper resource cleanup
- Differs from be_lo_creat by accepting a specific OID parameter
- Protected against execution in read-only transactions
- Part of PostgreSQL's large object API for creating binary data containers with specific identifiers
- If the requested OID is already in use, inv_create will assign a different OID and return it
- The created large object starts empty and can be written to using other lo_* functions