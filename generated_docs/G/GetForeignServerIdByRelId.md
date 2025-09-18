# GetForeignServerIdByRelId

## Location
src/backend/foreign/foreign.c: 355 - 376

## Overview
Retrieves the foreign server OID for a given foreign table relation ID by looking up the foreign table in the system catalog.

## Definition
```c
Oid GetForeignServerIdByRelId(Oid relid)
```

## Detailed Description
This function performs a system catalog lookup to find the foreign server associated with a specific foreign table. It searches the `pg_foreign_table` system catalog using the provided relation ID and returns the OID of the foreign server that hosts the table. The function is essential for foreign data wrapper operations that need to identify which foreign server a particular foreign table belongs to.

The function uses the system cache (FOREIGNTABLEREL) for efficient lookups and includes error handling for cases where the foreign table cannot be found.

## Parameters / Member Variables
- `relid`: The OID of the foreign table relation for which to find the associated foreign server

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - elog
  - ObjectIdGetDatum
  - GETSTRUCT
  - ReleaseSysCache
  - Form_pg_foreign_table
- Called from (representative examples):
  - ExecuteTruncateGuts
  - truncate_check_rel
  - GetFdwRoutineByRelId
  - get_relation_info

## Notes and Other Information
- Returns the foreign server OID (serverid field from pg_foreign_table)
- Throws an ERROR if the foreign table lookup fails
- Uses system cache for performance optimization
- Part of PostgreSQL's foreign data wrapper infrastructure