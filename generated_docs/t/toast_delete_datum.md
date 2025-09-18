# toast_delete_datum

## Location
[src/backend/access/common/toast_internals.c:385-460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_internals.c#L385-L460)

## Overview
Deletes all chunks of a single externally stored TOAST value from the secondary toast relation.

## Definition


## Detailed Description
This function removes a complete TOASTed value from the toast relation by locating and deleting all chunks that belong to the specified value. It extracts the toast pointer information from the input datum to identify the target toast relation and value ID, then performs a systematic scan to find and delete all associated chunk tuples.

The function supports both regular deletion and speculative deletion scenarios. Speculative deletion is used when removing tuples that were inserted speculatively (such as during unique constraint checking) and need to be aborted rather than simply deleted. The function uses the toast relation's index to efficiently locate all chunks belonging to the target value ID.

The deletion process maintains proper locking semantics, keeping locks until transaction commit to prevent conflicts with concurrent operations such as reindex on the toast relation.

## Parameters / Member Variables
- : The main relation (not the toast relation) that owns the external value
- : Datum containing the external toast pointer to be deleted
- : Boolean indicating whether to use speculative deletion (heap_abort_speculative) or regular deletion (simple_heap_delete)

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_ONDISK
  - VARATT_EXTERNAL_GET_POINTER
  - table_open
  - [toast_open_indexes](toast_open_indexes.md)
  - [toast_close_indexes](toast_close_indexes.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [init_toast_snapshot](../i/init_toast_snapshot.md)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md)
  - [systable_getnext_ordered](../s/systable_getnext_ordered.md)
  - [systable_endscan_ordered](../s/systable_endscan_ordered.md)
  - [heap_abort_speculative](../h/heap_abort_speculative.md)
  - [simple_heap_delete](../s/simple_heap_delete.md)
- Called from (representative examples):
  - [toast_tuple_cleanup](toast_tuple_cleanup.md)
  - [toast_delete_external](toast_delete_external.md)

## Notes and Other Information
- Only processes external on-disk toast values, silently returns for other value types
- Uses the toast pointer's va_valueid to locate all chunks belonging to the value
- Performs ordered scan using the toast relation's primary index for efficiency
- Maintains RowExclusiveLock on toast relation and indexes during the operation
- Supports both speculative and regular deletion modes for different transaction scenarios
- Uses init_toast_snapshot() to ensure proper visibility semantics during chunk scanning
- Keeps locks until commit to prevent concurrent reindex operations from interfering