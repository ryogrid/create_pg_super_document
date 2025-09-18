# pg_relation_filenode

## Location
src/backend/utils/adt/dbsize.c: 879 - 929

## Overview
This PostgreSQL SQL function retrieves the filenode number of a relation given its OID, working efficiently from the pg_class catalog without opening the actual relation.

## Definition
```c
Datum pg_relation_filenode(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_relation_filenode` function provides access to the underlying filesystem filenode number for a given relation OID. This is particularly useful for administrative queries and system analysis. The function implements several design choices for robustness:

1. **Catalog-only Operation**: Works directly from the pg_class system catalog row rather than opening relations for efficiency
2. **MVCC-aware Handling**: Gracefully handles cases where relations might be visible in the query snapshot but have been dropped 
3. **Storage Detection**: Only returns filenode numbers for relation kinds that actually have physical storage
4. **Mapper Integration**: For relations without explicit relfilenode values, consults the relation mapper to get the correct filenode
5. **Null Handling**: Returns NULL for relations without storage or when relations can't be found, rather than failing

The function retrieves the pg_class tuple, checks if the relation kind supports storage, and then either returns the direct relfilenode value or consults RelationMapOidToFilenumber for mapped relations (like system catalogs).

## Parameters / Member Variables
- Function accepts one argument via `PG_GETARG_OID(0)`: The OID of the relation whose filenode number is requested

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extracts OID argument from function call
  - SearchSysCache1: Searches system cache for tuple by single key
  - ObjectIdGetDatum: Converts OID to Datum
  - HeapTupleIsValid: Checks if heap tuple is valid
  - PG_RETURN_NULL: Returns NULL from function
  - GETSTRUCT: Extracts struct from heap tuple
  - RELKIND_HAS_STORAGE: Macro to check if relation kind has physical storage
  - RelationMapOidToFilenumber: Maps OID to filenode for mapped relations
  - InvalidRelFileNumber: Constant for invalid file number
  - ReleaseSysCache: Releases system cache tuple
  - RelFileNumberIsValid: Checks if file number is valid
  - PG_RETURN_OID: Returns OID result from function
- Called from (representative examples):
  - No direct references found (likely called via SQL)

## Notes and Other Information
This function is designed to be called from SQL as `pg_relation_filenode(oid)`. It's commonly used in administrative queries like `SELECT pg_relation_filenode(oid) FROM pg_class;` to examine the physical file layout of database relations. The function handles edge cases gracefully by returning NULL rather than errors, making it suitable for bulk queries across all relations. It distinguishes between relations with direct filenode storage and those that use the relation mapper (typically system catalogs). The function provides essential functionality for database administration, backup tools, and system monitoring utilities that need to understand the physical file layout. Located in src/backend/utils/adt/dbsize.c:879-929.