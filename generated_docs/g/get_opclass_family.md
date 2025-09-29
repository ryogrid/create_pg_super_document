# get_opclass_family

## Location
[src/backend/utils/cache/lsyscache.c:1190-1211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1190-L1211)

## Overview
Returns the OID of the operator family that contains the specified operator class.

## Definition
```c
Oid get_opclass_family(Oid opclass)
```

## Detailed Description
This function retrieves the operator family OID (opcfamily) for a given operator class OID from the pg_opclass system catalog. Operator classes belong to operator families, which group together semantically compatible operators and support functions for different data types. The function performs a system cache lookup and throws an error if the operator class is not found.

Operator families are a key concept in PostgreSQL's indexing infrastructure, allowing different data types that share similar comparison semantics to use the same set of operators and support functions. This function is commonly used when building indexes, performing type cache operations, and setting up replication scan keys.

## Parameters / Member Variables
- `opclass`: The OID of the operator class whose family OID is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_opclass

- Called from (representative examples):
  - [ComputeIndexAttrs](../C/ComputeIndexAttrs.md) (src/backend/commands/indexcmds.c:2102)
  - [build_replindex_scan_key](../b/build_replindex_scan_key.md) (src/backend/executor/execReplication.c:134)
  - [infer_collation_opclass_match](../i/infer_collation_opclass_match.md) (src/backend/optimizer/util/plancat.c:998)
  - [lookup_type_cache](../l/lookup_type_cache.md) (src/backend/utils/cache/typcache.c:488, 529)
  - [load_rangetype_info](../l/load_rangetype_info.md) (src/backend/utils/cache/typcache.c:943)

## Notes and Other Information
- Part of the OPCLASS CACHE section in lsyscache.c
- Throws an ERROR if the operator class OID is not found in the system catalog
- Critical for index creation and optimization operations
- Operator families enable cross-type comparisons and indexing strategies
- Used extensively in type cache management and replication infrastructure

## Simplified Source

```c
Oid
get_opclass_family(Oid opclass)
{
    HeapTuple tp;
    Form_pg_opclass cla_tup;
    Oid result;

    // Look up operator class in system cache
    tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));

    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for opclass %u", opclass);

    // Extract operator family OID from catalog entry
    cla_tup = (Form_pg_opclass) GETSTRUCT(tp);
    result = cla_tup->opcfamily;

    ReleaseSysCache(tp);
    return result;
}
```