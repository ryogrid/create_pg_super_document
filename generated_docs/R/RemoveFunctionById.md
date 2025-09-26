# RemoveFunctionById

## Location
[src/backend/commands/functioncmds.c:1293-1342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L1293-L1342)

## Overview
Removes a function or aggregate from the PostgreSQL system catalogs by deleting its entries from pg_proc and potentially pg_aggregate tables.

## Definition

```c
void
RemoveFunctionById(Oid funcOid)
```
## Detailed Description
RemoveFunctionById is the core function responsible for physically deleting function and aggregate definitions from PostgreSQL's system catalogs. It handles the low-level deletion of catalog tuples and statistics cleanup. The function works by:

1. Opening the pg_proc catalog table with exclusive row lock
2. Looking up the function tuple using the provided OID
3. Extracting the prokind field to determine if it's an aggregate
4. Deleting the pg_proc tuple from the catalog
5. Dropping associated statistics via pgstat_drop_function
6. If the function is an aggregate (prokind == PROKIND_AGGREGATE), also deleting the corresponding pg_aggregate tuple

This function is typically called by the dependency system during DROP operations and should not be called directly by user code.

## Parameters / Member Variables
- : The Object ID of the function or aggregate to be removed from the system catalogs

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - Form_pg_proc
  - GETSTRUCT
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [ReleaseSysCache](ReleaseSysCache.md)
  - [table_close](../t/table_close.md)
  - [pgstat_drop_function](../p/pgstat_drop_function.md)
  - PROKIND_AGGREGATE
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (dependency.c:1388)

## Notes and Other Information
- This function is used for both regular functions and aggregates since both use pg_proc as their primary catalog table
- The function assumes the caller has already handled dependency checking and permissions
- Statistics cleanup is performed via pgstat_drop_function to ensure monitoring data is properly removed
- Error handling includes cache lookup failures which should not normally occur
- The function uses RowExclusiveLock to ensure safe concurrent access to catalog tables