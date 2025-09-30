# get_opclass_input_type

## Location
[src/backend/utils/cache/lsyscache.c:1212-1234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1212-L1234)

## Overview
Returns the OID of the data type that the specified operator class is designed to index.

## Definition
```c
Oid get_opclass_input_type(Oid opclass)
```

## Detailed Description
This function retrieves the input data type OID (opcintype) for a given operator class OID from the pg_opclass system catalog. Each operator class is designed to handle indexing operations for a specific data type, and this function returns that associated data type. The function performs a system cache lookup and throws an error if the operator class is not found.

This function is essential for index operations, type validation, and ensuring compatibility between data types and their indexing strategies. It's commonly used when building indexes, validating operator class compatibility, and setting up replication infrastructure where type matching is critical.

## Parameters / Member Variables
- `opclass`: The OID of the operator class whose input data type OID is to be retrieved

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
  - [hashadjustmembers](../h/hashadjustmembers.md) (src/backend/access/hash/hashvalidate.c:385)
  - [btadjustmembers](../b/btadjustmembers.md) (src/backend/access/nbtree/nbtvalidate.c:326)
  - [CheckIndexCompatible](../C/CheckIndexCompatible.md) (src/backend/commands/indexcmds.c:298)
  - [findRangeSubOpclass](../f/findRangeSubOpclass.md) (src/backend/commands/typecmds.c:2295)
  - [build_replindex_scan_key](../b/build_replindex_scan_key.md) (src/backend/executor/execReplication.c:133)
  - [infer_collation_opclass_match](../i/infer_collation_opclass_match.md) (src/backend/optimizer/util/plancat.c:999)
  - [get_rule_expr](get_rule_expr.md) (src/backend/utils/adt/ruleutils.c:10112)
  - [lookup_type_cache](../l/lookup_type_cache.md) (src/backend/utils/cache/typcache.c:489, 530)
  - [load_rangetype_info](../l/load_rangetype_info.md) (src/backend/utils/cache/typcache.c:944)

## Notes and Other Information
- Part of the OPCLASS CACHE section in lsyscache.c
- Throws an ERROR if the operator class OID is not found in the system catalog
- Critical for ensuring type safety in indexing operations
- Used extensively in index validation (hash, btree) and compatibility checking
- Essential for replication infrastructure where exact type matching is required
- The opcintype field represents the "input type" that the operator class can handle

## Simplified Source

```c
// Simplified version of get_opclass_input_type
Oid
get_opclass_input_type(Oid opclass)
{
    HeapTuple tuple;
    Form_pg_opclass opclass_form;
    Oid result;

    // Look up the operator class in system cache
    tuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for opclass %u", opclass);

    // Extract the input type from the operator class record
    opclass_form = (Form_pg_opclass) GETSTRUCT(tuple);
    result = opclass_form->opcintype;

    // Clean up and return
    ReleaseSysCache(tuple);
    return result;
}
```

Key simplifications made:
- Used more descriptive variable names (tuple instead of tp, opclass_form instead of cla_tup)
- Added clear comments explaining each step
- Preserved all error handling as it's essential for this function
- Simplified the structure while maintaining the exact same logic