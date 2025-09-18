# get_constraint_type

## Location
src/backend/utils/cache/lsyscache.c: 1143 - 1160

## Overview
Returns the constraint type character (contype) for a given constraint OID from the pg_constraint system catalog.

## Definition
```c
char get_constraint_type(Oid conoid)
```

## Detailed Description
This is a straightforward utility function that retrieves the constraint type from the pg_constraint system catalog given a constraint OID. The function performs a system cache lookup and returns the contype field directly. Unlike some other lsyscache functions, this one throws an error if the constraint is not found, as indicated by the "No frills" comment - it assumes the caller has verified the constraint exists.

The returned character represents the constraint type using PostgreSQL's internal encoding (e.g., 'u' for unique, 'p' for primary key, 'f' for foreign key, 'c' for check, 'x' for exclusion, etc.).

## Parameters / Member Variables
- `conoid`: The OID of the constraint whose type is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_constraint

- Called from (representative examples):
  - [AttachPartitionEnsureIndexes](../A/AttachPartitionEnsureIndexes.md) (src/backend/commands/tablecmds.c:18931, 18932)

## Notes and Other Information
- This function will raise an ERROR if the constraint OID is not found in the system catalog
- The returned character follows PostgreSQL's constraint type encoding scheme
- Part of the lsyscache utility functions for efficient system catalog access
- The "No frills" comment indicates this is a basic lookup function without error handling for missing constraints