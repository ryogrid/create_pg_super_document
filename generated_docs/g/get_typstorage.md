# get_typstorage

## Location
[src/backend/utils/cache/lsyscache.c:2419-2447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2419-L2447)

## Overview
Retrieves the storage strategy for a specified PostgreSQL data type from the system catalog.

## Definition

```c
struct fields. Must do it the hard way with
	 * SysCacheGetAttr.
	 */
	datum = SysCacheGetAttr(TYPEOID,
							typeTuple,
							Anum_pg_type_typdefaultbin,
							&isNull);
```
## Detailed Description
This function performs a system catalog lookup to retrieve the storage strategy for a given data type. The storage strategy determines how PostgreSQL handles the storage of values of this type, particularly for variable-length types that may be subject to compression or out-of-line storage (TOAST). The function queries the pg_type system catalog using the syscache mechanism for efficient access. If the type is not found in the catalog, it returns TYPSTORAGE_PLAIN as a default value, indicating plain storage without compression or external storage.

## Parameters / Member Variables
- : The OID of the data type whose storage strategy is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - Form_pg_type
  - TYPSTORAGE_PLAIN
- Called from (representative examples):
  - [GetAttributeStorage](../G/GetAttributeStorage.md)
  - TypeIsToastable

## Notes and Other Information
- Returns one of four possible storage strategies: 'p' (plain), 'e' (external), 'm' (main), or 'x' (extended)
- 'p' (TYPSTORAGE_PLAIN): Values are always stored inline and never compressed
- 'e' (TYPSTORAGE_EXTERNAL): Values can be stored out-of-line but not compressed
- 'm' (TYPSTORAGE_MAIN): Values can be compressed but preferably stored inline
- 'x' (TYPSTORAGE_EXTENDED): Values can be compressed and stored out-of-line
- The default return value TYPSTORAGE_PLAIN ensures safe handling for unknown types
- Part of the lsyscache.c module which provides cached access to system catalog information
- This information is crucial for PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system
- Used by the storage layer to determine appropriate storage and compression strategies for large values