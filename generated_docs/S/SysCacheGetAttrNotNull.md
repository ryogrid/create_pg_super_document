# SysCacheGetAttrNotNull

## Location
src/backend/utils/cache/syscache.c: 632 - 661

## Overview
Extracts a specific attribute from a system cache tuple, with the guarantee that the attribute cannot be NULL.

## Definition
```c
Datum SysCacheGetAttrNotNull(int cacheId, HeapTuple tup, AttrNumber attributeNumber)
```

## Detailed Description
This function is a specialized version of SysCacheGetAttr that is used when the caller knows that the requested attribute cannot be NULL. It internally calls SysCacheGetAttr to perform the attribute extraction, but adds an additional safety check - if the attribute value is unexpectedly NULL, it raises an ERROR with detailed information about which catalog and column produced the unexpected NULL value.

This function provides both performance benefits (no need to check the isNull output parameter) and safety benefits (automatic detection of data integrity issues) for cases where NULL values should never occur for specific attributes.

## Parameters / Member Variables
- `cacheId`: The cache ID referencing the system cache containing the tuple
- `tup`: The HeapTuple previously fetched by SearchSysCache() or similar functions  
- `attributeNumber`: The attribute number to extract from the tuple (must be non-NULL)

## Dependencies
- Functions called/Symbols referenced:
  - SysCacheGetAttr
  - elog
  - get_rel_name
  - NameStr
  - TupleDescAttr
- Called from (representative examples):
  - inclusion_get_strategy_procinfo (src/backend/access/brin/brin_inclusion.c:650)
  - ExecGrant_common (src/backend/catalog/aclchk.c:2206)
  - ProcedureCreate (src/backend/catalog/pg_proc.c:521)
  - get_db_info (src/backend/commands/dbcommands.c:2888)
  - ATExecValidateConstraint (src/backend/commands/tablecmds.c:11841)

## Notes and Other Information
- Provides better error diagnostics than manually checking SysCacheGetAttr results
- Should only be used when the caller is certain that the attribute cannot be NULL based on catalog design
- The error message includes both the relation name and column name for easier debugging
- Widely used throughout PostgreSQL for accessing mandatory system catalog attributes
- Part of the system cache infrastructure optimized for non-nullable attribute access
- The error is raised using elog(ERROR, ...) which will abort the current transaction if the unexpected NULL is encountered