# CheckAndCreateToastTable

## Location
[src/backend/catalog/toasting.c:78-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/toasting.c#L78-L97)

## Overview
CheckAndCreateToastTable is a static function that serves as the common implementation backend for all TOAST table creation variants, handling relation opening, delegation to create_toast_table, and cleanup.

## Definition

```c
static void
CheckAndCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode,
						 bool check, Oid OIDOldToast)
```
## Detailed Description
This is the core implementation function that underlies all the public TOAST table creation functions. It provides a unified interface that handles the common pattern of opening a relation, calling create_toast_table to do the actual work, and then properly closing the relation. The function acts as an adapter between the public API functions and the lower-level create_toast_table implementation.

The function takes care of proper resource management by opening the relation with the specified lock mode and ensuring it's properly closed afterward. It passes through all necessary parameters to create_toast_table, including reloptions for customizing the TOAST table, lock mode for concurrency control, check parameter for validation behavior, and old TOAST OID for operations that need to reference previous TOAST tables.

## Parameters / Member Variables
- `relOid`: The OID of the relation for which to potentially create a TOAST table
- `reloptions`: Datum containing reloptions for the TOAST table configuration
- `lockmode`: The lock mode to use when accessing the relation
- `check`: Boolean flag controlling validation behavior (true for ALTER TABLE scenarios, false for new relation scenarios)
- `OIDOldToast`: The OID of an existing TOAST table, if any (used for table rebuilding operations)
## Dependencies
- Functions called/Symbols referenced:
  - [create_toast_table](../c/create_toast_table.md)
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [AlterTableCreateToastTable](../A/AlterTableCreateToastTable.md) (in src/backend/catalog/toasting.c:60)
  - [NewHeapCreateToastTable](../N/NewHeapCreateToastTable.md) (in src/backend/catalog/toasting.c:67)
  - [NewRelationCreateToastTable](../N/NewRelationCreateToastTable.md) (in src/backend/catalog/toasting.c:73)

## Notes and Other Information
- This is a static function, not part of the public API - it serves as an implementation detail
- Handles proper resource management by opening and closing relations with appropriate locking
- Acts as a bridge between the public API functions and the core create_toast_table implementation
- The InvalidOid parameters passed to create_toast_table indicate that TOAST table and index OIDs should be automatically assigned
- The NoLock parameter for table_close indicates the lock should be retained as acquired during table_open

## Simplified Source

```c
static void
CheckAndCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode,
                         bool check, Oid OIDOldToast)
{
    // Open the relation with specified lock mode
    Relation rel = table_open(relOid, lockmode);

    // Delegate all TOAST table creation work to create_toast_table
    // Use InvalidOid for automatic OID assignment of TOAST table and index
    (void) create_toast_table(rel, InvalidOid, InvalidOid, reloptions, lockmode,
                              check, OIDOldToast);

    // Close relation but retain the lock acquired during open
    table_close(rel, NoLock);
}
```