# get_typalign

## Location
[src/backend/utils/cache/lsyscache.c:2399-2418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2399-L2418)

## Overview
Retrieves the alignment requirement for a specified PostgreSQL data type from the system catalog.

## Definition

```c
char
get_typalign(Oid typid)
```
## Detailed Description
This function performs a system catalog lookup to retrieve the alignment requirement for a given data type. Type alignment specifies the memory alignment boundary that values of this type must respect when stored in memory or on disk. The function queries the pg_type system catalog using the syscache mechanism for efficient access. If the type is not found in the catalog, it returns a default alignment of TYPALIGN_INT as a fallback value.

## Parameters / Member Variables
- `typid`: The OID of the data type whose alignment requirement is to be retrieved
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - Form_pg_type
  - TYPALIGN_INT
- Called from (representative examples):
  - (No direct references found in the analyzed codebase)

## Notes and Other Information
- Returns one of four possible alignment values: 'c' (char/byte), 's' (short/2-byte), 'i' (int/4-byte), or 'd' (double/8-byte)
- The default return value TYPALIGN_INT ('i') provides 4-byte alignment for unknown types
- Part of the lsyscache.c module which provides cached access to frequently-needed system catalog information
- This function is used internally by PostgreSQL's type system to ensure proper memory alignment when handling values
- Proper alignment is crucial for performance and correctness on architectures that require aligned memory access

## Simplified Source

```c
char get_typalign(Oid typid) {
    // Look up type in system cache
    HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(tp)) {
        // Extract pg_type struct and get alignment requirement
        Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
        char result = typtup->typalign;

        // Clean up cache reference
        ReleaseSysCache(tp);
        return result;
    } else {
        // Return default int alignment if type not found
        return TYPALIGN_INT;
    }
}
```