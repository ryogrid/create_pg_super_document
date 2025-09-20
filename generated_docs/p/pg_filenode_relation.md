# pg_filenode_relation

## Location
[src/backend/utils/adt/dbsize.c:930-953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L930-L953)

## Overview
A PostgreSQL system function that maps a filesystem file back to its corresponding relation (table/index) OID by taking a tablespace OID and relfilenumber as input parameters.

## Definition

```c
enumber = PG_GETARG_OID(1);
```
## Detailed Description
The pg_filenode_relation function provides a mechanism to reverse-map filesystem files to their corresponding database relations. This is particularly useful when analyzing individual files on the filesystem and needing to identify which table or index they belong to. The function takes a tablespace OID and relfilenumber, then uses the internal RelidByRelfilenumber function to perform the lookup.

The function handles edge cases gracefully by returning NULL when no mapping can be found, when invalid relfilenumbers are provided, or when dealing with temporary relations (which are intentionally not detected for consistency with RelidByRelfilenumber behavior). The function also supports passing InvalidOid for the tablespace parameter to represent the current database's default tablespace.

This function is especially valuable for database administration tasks, forensic analysis of database files, and tools that need to correlate filesystem-level information with logical database structures.

## Parameters / Member Variables
-  (Oid): The tablespace OID where the relation file resides. InvalidOid can be passed to represent the current database's default tablespace
-  (RelFileNumber): The file number of the relation file on disk

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileNumber](../R/RelFileNumber.md) (type)
  - RelFileNumberIsValid
  - RelidByRelfilenumber
  - PG_RETURN_OID
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Returns NULL rather than failing when no mapping can be found
- Does not detect temporary relations, returning NULL instead (consistent with RelidByRelfilenumber behavior)
- Validates relfilenumber input before processing to prevent RelidByRelfilenumber misbehavior
- Part of the database size and file system utility functions in PostgreSQL
- Located in src/backend/utils/adt/dbsize.c:930-953