# heap_toast_delete

## Location
[src/backend/access/heap/heaptoast.c:43-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heaptoast.c#L43-L95)

## Overview
A cascaded delete function that removes TOAST entries when a heap tuple is deleted from a relation.

## Definition


## Detailed Description
The `heap_toast_delete` function is responsible for cleaning up externally stored TOAST data when a heap tuple is deleted. It performs cascaded deletion of toast entries to maintain data consistency and prevent orphaned TOAST chunks in the toast table.

The function decomposes the tuple being deleted into its constituent attributes using `heap_deform_tuple`, then delegates the actual deletion work to `toast_delete_external`. This design separates the heap-level deletion logic from the low-level TOAST storage management.

The function includes safety checks to ensure it's only called on appropriate relation types (regular tables and materialized views) and uses an efficient O(N) approach for tuple decomposition rather than O(N^2) repeated `heap_getattr` calls.

## Parameters / Member Variables
- `rel`: The relation containing the tuple being deleted
- `oldtup`: The heap tuple being deleted that may contain TOAST references
- `is_speculative`: Whether this is a speculative deletion (used for conflict resolution)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_deform_tuple](heap_deform_tuple.md)
  - [toast_delete_external](../t/toast_delete_external.md)
  - MaxHeapAttributeNumber
  - RELKIND_RELATION
  - RELKIND_MATVIEW
- Called from (representative examples):
  - [heap_delete](heap_delete.md)
  - [heap_abort_speculative](heap_abort_speculative.md)

## Notes and Other Information
- Only operates on plain relations (RELKIND_RELATION) and materialized views (RELKIND_MATVIEW)
- Uses linear-time tuple decomposition for efficiency when multiple varlena columns are present
- The function won't be called unless there's at least one varlena column in the tuple
- Part of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system for handling large column values