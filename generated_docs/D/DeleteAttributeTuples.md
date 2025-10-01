# DeleteAttributeTuples

## Location
[src/backend/catalog/heap.c:1588-1624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L1588-L1624)

## Overview
Removes all pg_attribute catalog entries associated with a specified relation during relation or index deletion operations.

## Definition

```c
void
DeleteAttributeTuples(Oid relid)
```
## Detailed Description
This function is responsible for cleaning up column metadata when a relation or index is being dropped from the database. It systematically removes all attribute definitions stored in the pg_attribute system catalog that belong to the specified relation.

The function performs the following operations:

1. **Catalog Access**: Opens the pg_attribute relation with RowExclusiveLock to ensure exclusive access during the deletion process.

2. **Indexed Scan Setup**: Initializes a scan key to find all attribute entries belonging to the target relation by matching on the attrelid column (which stores the relation OID that each attribute belongs to).

3. **Efficient Scanning**: Uses the AttributeRelidNumIndexId index to perform an efficient lookup of all attributes for the given relation, rather than scanning the entire pg_attribute catalog.

4. **Bulk Deletion**: Iterates through all matching attribute tuples and deletes each one using CatalogTupleDelete, which handles both the physical tuple removal and any associated index maintenance.

5. **Cleanup**: Properly ends the system scan and closes the pg_attribute relation.

This function is designed to be shared between relation deletion and index deletion workflows, as both types of database objects have attribute definitions in pg_attribute that must be removed when the object is dropped. The function removes all attributes associated with the relation, including both user-defined columns and system-defined attributes.

## Parameters / Member Variables
- : The OID of the relation whose pg_attribute entries should be deleted

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (opens pg_attribute catalog for modification)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes scan key for relation ID lookup)
  - [systable_beginscan](../s/systable_beginscan.md) (begins indexed scan on pg_attribute)
  - [systable_getnext](../s/systable_getnext.md) (retrieves next matching attribute tuple)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (deletes attribute tuple from catalog)
  - [systable_endscan](../s/systable_endscan.md) (ends the system catalog scan)
  - [table_close](../t/table_close.md) (closes pg_attribute catalog relation)
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md) (during table deletion)
  - [index_drop](../i/index_drop.md) (during index deletion)

## Notes and Other Information
- This function is explicitly documented as shared between relation and index deletion and is not intended for use in other contexts
- Uses the AttributeRelidNumIndexId index for efficient lookup of all attributes belonging to a specific relation
- The RowExclusiveLock ensures that no other transactions can modify the pg_attribute catalog during the deletion operation
- Deletes all types of attributes including user-defined columns, system columns, and dropped columns (which are marked as dropped but still have catalog entries)
- This operation is typically performed before DeleteRelationTuple, as part of the comprehensive cleanup process when dropping database objects
- The function handles both regular table attributes and index attributes, making it a versatile component in PostgreSQL's object deletion infrastructure
- Each CatalogTupleDelete call also maintains any indexes on pg_attribute, ensuring catalog consistency throughout the deletion process

## Simplified Source

```c
void DeleteAttributeTuples(Oid relid)
{
    Relation attrel;
    SysScanDesc scan;
    ScanKeyData key[1];
    HeapTuple atttup;

    // Open pg_attribute catalog with exclusive lock
    attrel = table_open(AttributeRelationId, RowExclusiveLock);

    // Set up scan key to find all attributes for this relation
    ScanKeyInit(&key[0], Anum_pg_attribute_attrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    // Start indexed scan for efficient attribute lookup
    scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true,
                              NULL, 1, key);

    // Delete all matching attribute tuples
    while ((atttup = systable_getnext(scan)) != NULL)
        CatalogTupleDelete(attrel, &atttup->t_self);

    // Clean up scan and close catalog
    systable_endscan(scan);
    table_close(attrel, RowExclusiveLock);
}
```