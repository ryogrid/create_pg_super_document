# EnumValuesDelete

## Location
[src/backend/catalog/pg_enum.c:224-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L224-L254)

## Overview
Removes all pg_enum entries for a specified enum type during enum type deletion operations.

## Definition

```c
void
EnumValuesDelete(Oid enumTypeOid)
```
## Detailed Description
EnumValuesDelete is a cleanup function that removes all enum value entries from the pg_enum catalog table for a given enum type. This function is typically called during DROP TYPE operations to ensure complete removal of enum-related catalog entries.

The function performs a systematic scan of the pg_enum table using the enum type OID as the search key, finding all enum values associated with the specified type and deleting them one by one. It uses the EnumTypIdLabelIndexId index for efficient lookup of all enum values belonging to the specified enum type.

The deletion process maintains catalog consistency by properly removing both the heap tuples and updating all associated indexes through the CatalogTupleDelete function.

## Parameters / Member Variables
- `enumTypeOid`: The OID of the enum type whose values should be deleted from pg_enum
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
- Called from:
  - [RemoveTypeById](../R/RemoveTypeById.md) (src/backend/commands/typecmds.c:676)

## Notes and Other Information
- The function uses RowExclusiveLock on the pg_enum table to ensure exclusive access during deletion
- Uses the EnumTypIdLabelIndexId index for efficient scanning of enum values by type OID
- Each enum value tuple is deleted individually using CatalogTupleDelete to maintain index consistency
- This function is part of the enum type cleanup process and should only be called when the enum type itself is being dropped
- The function does not perform any validation - it assumes the caller has verified that the enum type deletion is appropriate

## Simplified Source

```c
void EnumValuesDelete(Oid enumTypeOid) {
    Relation pg_enum;
    ScanKeyData key[1];
    SysScanDesc scan;
    HeapTuple tup;

    // Open pg_enum catalog for modification
    pg_enum = table_open(EnumRelationId, RowExclusiveLock);

    // Set up scan key to find all enum values for this type
    ScanKeyInit(&key[0], Anum_pg_enum_enumtypid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(enumTypeOid));

    // Begin indexed scan using EnumTypIdLabelIndexId
    scan = systable_beginscan(pg_enum, EnumTypIdLabelIndexId, true,
                             NULL, 1, key);

    // Delete all enum values for this type
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        CatalogTupleDelete(pg_enum, &tup->t_self);
    }

    // Clean up scan and close catalog
    systable_endscan(scan);
    table_close(pg_enum, RowExclusiveLock);
}
```