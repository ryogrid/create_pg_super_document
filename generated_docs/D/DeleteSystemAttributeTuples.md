# DeleteSystemAttributeTuples

## Location
[src/backend/catalog/heap.c:1625-1665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L1625-L1665)

## Overview
Removes pg_attribute entries for system columns of a specified relation, primarily used when converting tables to views.

## Definition

```c
void
DeleteSystemAttributeTuples(Oid relid)
```
## Detailed Description
This function is a specialized catalog maintenance utility designed specifically for the table-to-view conversion process in PostgreSQL. Unlike regular tables, views do not have system columns (such as ctid, oid, tableoid, etc.), so when a table is converted to a view, these system attribute definitions must be removed from the pg_attribute catalog.

The function performs a targeted deletion operation:

1. **Catalog Access**: Opens the pg_attribute relation with RowExclusiveLock to ensure exclusive access during the deletion process.

2. **Dual-Condition Scan**: Sets up a two-part scan key to identify system attributes:
   - First condition: matches the target relation ID (attrelid column)
   - Second condition: identifies system columns by their negative or zero attribute numbers (attnum <= 0)

3. **System Column Identification**: PostgreSQL uses a convention where user-defined columns have positive attribute numbers (attnum > 0), while system columns have non-positive attribute numbers (attnum <= 0). This includes system columns like:
   - ctid (physical location identifier)
   - oid (object identifier, when enabled)
   - tableoid (table OID for inheritance hierarchies)
   - xmin, xmax (transaction visibility information)

4. **Indexed Deletion**: Uses the AttributeRelidNumIndexId index for efficient lookup and deletes all matching system attribute tuples using CatalogTupleDelete.

5. **Cleanup**: Properly ends the scan and closes the pg_attribute relation.

This function is highly specialized and is only used in the specific context of table-to-view conversions, where the structural change from a storage-backed table to a query-backed view necessitates the removal of storage-related system columns.

## Parameters / Member Variables
- `relid`: The OID of the relation whose system attribute entries should be deleted from pg_attribute
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (opens pg_attribute catalog for modification)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes scan keys for relation ID and attribute number conditions)
  - [systable_beginscan](../s/systable_beginscan.md) (begins indexed scan on pg_attribute with dual conditions)
  - [systable_getnext](../s/systable_getnext.md) (retrieves next matching system attribute tuple)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (deletes system attribute tuple from catalog)
  - [systable_endscan](../s/systable_endscan.md) (ends the system catalog scan)
  - [table_close](../t/table_close.md) (closes pg_attribute catalog relation)
- Called from (representative examples):
  - Currently no direct callers found in the codebase (may be used in table-to-view conversion logic)

## Notes and Other Information
- This function is specifically designed for table-to-view conversions and should not be used in other contexts
- Uses a dual-condition scan with both relation ID matching and attribute number filtering (attnum <= 0) to target only system columns
- The BTLessEqualStrategyNumber strategy on attnum ensures that only system attributes (non-positive attribute numbers) are selected
- System columns in PostgreSQL have special negative or zero attribute numbers that distinguish them from user-defined columns
- The function preserves user-defined columns (attnum > 0) while removing only the system-generated attributes that are inappropriate for views
- This operation is part of the broader process of converting table metadata to view metadata, which involves multiple catalog changes
- The RowExclusiveLock ensures that no concurrent modifications can occur to the pg_attribute catalog during the deletion process
- Unlike DeleteAttributeTuples which removes all attributes, this function is surgical in removing only system attributes while preserving user-defined column metadata

## Simplified Source

```c
void DeleteSystemAttributeTuples(Oid relid) {
    Relation attrel;
    SysScanDesc scan;
    ScanKeyData key[2];
    HeapTuple atttup;

    // Open pg_attribute catalog for modification
    attrel = table_open(AttributeRelationId, RowExclusiveLock);

    // Set up scan to find system attributes (attnum <= 0) for this relation
    ScanKeyInit(&key[0], Anum_pg_attribute_attrelid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&key[1], Anum_pg_attribute_attnum,
                BTLessEqualStrategyNumber, F_INT2LE, Int16GetDatum(0));

    // Begin indexed scan to find system attributes
    scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true,
                              NULL, 2, key);

    // Delete all system attribute tuples found
    while ((atttup = systable_getnext(scan)) != NULL) {
        CatalogTupleDelete(attrel, &atttup->t_self);
    }

    // Clean up
    systable_endscan(scan);
    table_close(attrel, RowExclusiveLock);
}
```