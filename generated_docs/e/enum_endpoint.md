# enum_endpoint

## Location
[src/backend/utils/adt/enum.c:392-436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L392-L436)

## Overview
A static helper function that implements common code for finding the first or last member of an enum type by scanning the pg_enum system catalog in a specified direction.

## Definition

```c
static Oid
enum_endpoint(Oid enumtypoid, ScanDirection direction)
```
## Detailed Description
The  function serves as the core implementation for both  and  SQL functions. It performs an ordered scan of the pg_enum system catalog to find either the first or last enum value based on the specified scan direction. The function uses the  to ensure proper ordering and explicitly avoids the system cache for safety reasons related to concurrent enum modifications.

The function implements proper transaction safety by calling  to ensure that uncommitted enum values are not used in SQL operations, preventing potential index corruption during transaction rollbacks.

## Parameters / Member Variables
- `enumtypoid`: The OID of the enum type for which to find the endpoint value
- `direction`: The scan direction (ForwardScanDirection for first, BackwardScanDirection for last)
## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_open](../t/table_open.md)
  - [index_open](../i/index_open.md)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md)
  - [systable_getnext_ordered](../s/systable_getnext_ordered.md)
  - [check_safe_enum_use](../c/check_safe_enum_use.md)
  - [systable_endscan_ordered](../s/systable_endscan_ordered.md)
  - [index_close](../i/index_close.md)
  - [table_close](../t/table_close.md)
- Called from:
  - [enum_first](enum_first.md)
  - [enum_last](enum_last.md)

## Notes and Other Information
- This is a static function, not directly accessible outside the enum.c module
- Explicitly avoids using the system cache due to concurrency concerns with enum renumbering operations
- Returns InvalidOid for empty enum types
- Uses ordered scanning with the pg_enum_typid_sortorder_index for consistent results
- Implements proper resource cleanup by closing relations and indexes after use

## Simplified Source

```c
static Oid enum_endpoint(Oid enumtypoid, ScanDirection direction) {
    // Initialize scan key for enum type
    ScanKeyData skey;
    ScanKeyInit(&skey, Anum_pg_enum_enumtypid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(enumtypoid));

    // Open pg_enum table and its sort order index
    Relation enum_rel = table_open(EnumRelationId, AccessShareLock);
    Relation enum_idx = index_open(EnumTypIdSortOrderIndexId, AccessShareLock);

    // Start ordered scan to find first/last enum value
    SysScanDesc enum_scan = systable_beginscan_ordered(enum_rel, enum_idx, NULL, 1, &skey);

    // Get the first tuple in the specified direction
    HeapTuple enum_tuple = systable_getnext_ordered(enum_scan, direction);
    Oid result = InvalidOid;

    if (HeapTupleIsValid(enum_tuple)) {
        // Ensure enum value is safe to use in SQL
        check_safe_enum_use(enum_tuple);
        result = ((Form_pg_enum) GETSTRUCT(enum_tuple))->oid;
    }

    // Clean up resources
    systable_endscan_ordered(enum_scan);
    index_close(enum_idx, AccessShareLock);
    table_close(enum_rel, AccessShareLock);

    return result;
}
```