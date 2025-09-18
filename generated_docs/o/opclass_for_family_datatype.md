# opclass_for_family_datatype

## Location
[src/backend/access/index/amvalidate.c:236-270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/amvalidate.c#L236-L270)

## Overview
Finds the OID of an operator class that belongs to a specified operator family and accepts a given data type as input.

## Definition
```c
Oid opclass_for_family_datatype(Oid amoid, Oid opfamilyoid, Oid datatypeoid)
```

## Detailed Description
This function searches through all operator classes belonging to a specific access method to find one that matches both the specified operator family and input data type. The search is performed by iterating through all opclasses for the access method, which is somewhat inefficient but necessary due to the lack of a better index. The function returns InvalidOid if no matching operator class is found. If multiple matching opclasses exist (which shouldn't happen), it returns an arbitrary one without additional checks.

## Parameters / Member Variables
- `amoid`: OID of the access method to search within
- `opfamilyoid`: OID of the operator family to match
- `datatypeoid`: OID of the data type that should be the opclass input type

## Dependencies
- Functions called/Symbols referenced:
  - CatCList (catalog cache list structure)
  - SearchSysCacheList1 (system cache list lookup)
  - Form_pg_opclass (operator class catalog tuple form)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md) (cache list release)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to datum conversion)
  - GETSTRUCT (macro to extract tuple structure)
  - InvalidOid (invalid OID constant)
- Called from (representative examples):
  - [hashadjustmembers](../h/hashadjustmembers.md)
  - [opfamily_can_sort_type](opfamily_can_sort_type.md)
  - [btadjustmembers](../b/btadjustmembers.md)

## Notes and Other Information
- Returns InvalidOid if no matching operator class is found
- If multiple matches exist, returns an arbitrary one (this shouldn't happen normally)
- Search method is somewhat inefficient but necessary due to index limitations
- Implicitly validates that the operator family belongs to the specified access method
- Part of the access method validation and adjustment infrastructure
- Located in src/backend/access/index/amvalidate.c:236-270