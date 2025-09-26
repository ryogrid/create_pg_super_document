# DeleteInheritsTuple

## Location
[src/backend/catalog/pg_inherits.c:552-619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_inherits.c#L552-L619)

## Overview
Deletes inheritance relationship entries from the pg_inherits system catalog based on specified criteria and validates detach pending state.

## Definition
```c
bool DeleteInheritsTuple(Oid inhrelid, Oid inhparent, bool expect_detach_pending, const char *childname)
```

## Detailed Description
This function removes inheritance relationship entries from the pg_inherits catalog table. It provides flexible deletion criteria and includes validation for partition detach operations. The function can delete either all inheritance relationships for a given child relation or only specific parent-child relationships.

Key features:
- Can delete all inheritance entries for a relation or filter by specific parent
- Validates the detach pending state for partition management operations  
- Provides detailed error messages for partition detach state mismatches
- Returns whether any rows were actually deleted
- Uses proper locking and catalog management functions

The function scans pg_inherits by inhrelid (child relation) and optionally filters by inhparent. Before deletion, it validates that the detach pending state matches expectations, which is crucial for concurrent partition detach operations.

## Parameters / Member Variables
- `inhrelid`: OID of the child relation whose inheritance entries should be deleted
- `inhparent`: OID of the parent relation to filter by, or InvalidOid to delete all parents for the child
- `expect_detach_pending`: Expected state of the inhdetachpending flag - function will error if actual state doesn't match
- `childname`: Name of the child partition for error messages, or NULL for regular inheritance

## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md): Begins system table scan on pg_inherits
  - [systable_getnext](../s/systable_getnext.md): Gets next tuple from the system scan
  - Form_pg_inherits: Accesses structured data from pg_inherits tuple
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Deletes the tuple from the system catalog
- Called from (representative examples):
  - [index_concurrently_swap](../i/index_concurrently_swap.md): During concurrent index operations
  - [index_drop](../i/index_drop.md): When dropping indexes with inheritance
  - [RemoveInheritance](../R/RemoveInheritance.md): Higher-level inheritance removal function

## Notes and Other Information
- Returns true if at least one row was deleted, false otherwise
- Critical for partition detach operations where concurrency control is important
- The expect_detach_pending parameter prevents race conditions in concurrent partition detach
- Error messages provide helpful hints for resolving partition detach issues
- Uses RowExclusiveLock on pg_inherits to ensure consistency
- Handles both regular table inheritance and partitioning scenarios
- The childname parameter is primarily used for meaningful error messages in partition operations
- Location: src/backend/catalog/pg_inherits.c:552-619