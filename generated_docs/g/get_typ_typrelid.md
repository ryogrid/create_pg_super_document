# get_typ_typrelid

## Location
src/backend/utils/cache/lsyscache.c: 2731 - 2758

## Overview
A system cache utility function that retrieves the relation OID (typrelid) associated with a given PostgreSQL type OID, returning InvalidOid for non-complex types.

## Definition
```c
Oid get_typ_typrelid(Oid typid)
```

## Detailed Description
This function performs a system catalog lookup to fetch the typrelid field from the pg_type system catalog. The typrelid field contains the OID of the relation (table) that defines a complex type's structure. For simple types (like integers, text, etc.), this field is InvalidOid (0). For complex types (composite types, table row types), it points to the pg_class entry that describes the type's structure. Unlike some other lsyscache functions, this function returns InvalidOid rather than throwing an error when the type is not found.

## Parameters / Member Variables
- `typid`: The OID (object identifier) of the PostgreSQL type to look up

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - ObjectIdGetDatum
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
  - Form_pg_type
  - InvalidOid
- Called from (representative examples):
  - find_expr_references_walker
  - process_function_rte_ref
  - CheckAttributeType
  - ATPostAlterTypeCleanup
  - processIndirection

## Notes and Other Information
This function is essential for distinguishing between simple and complex types in PostgreSQL. It's commonly used in dependency tracking, type validation, and situations where the system needs to understand the internal structure of a type. The function's graceful handling of invalid type OIDs (returning InvalidOid instead of throwing an error) makes it suitable for defensive programming contexts where type existence is uncertain.