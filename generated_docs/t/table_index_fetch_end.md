# table_index_fetch_end

## Location
[src/include/access/tableam.h:1212-1241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1212-L1241)

## Overview
Releases resources and deallocates an index fetch operation by cleaning up the IndexFetchTableData structure.

## Definition

```c
struct IndexFetchTableData *scan)
{
	scan->rel->rd_tableam->index_fetch_end(scan);
}

/*
 * Fetches, as part of an index scan, tuple at `tid` into `slot`, after doing
 * a visibility test according to `snapshot`. If a tuple was found and passed
 * the visibility test, returns true, false otherwise. Note that *tid may be
 * modified when we return true (see later remarks on multiple row versions
 * reachable via a single index entry).
 *
 * *call_again needs to be false on the first call to table_index_fetch_tuple() for
 * a tid. If there potentially is another tuple matching the tid, *call_again
 * will be set to true, signaling that table_index_fetch_tuple() should be called
 * again for the same tid.
 *
 * *all_dead, if all_dead is not NULL, will be set to true by
 * table_index_fetch_tuple() iff it is guaranteed that no backend needs to see
 * that tuple. Index AMs can use that to avoid returning that tid in future
 * searches.
 *
 * The difference between this function and table_tuple_fetch_row_version()
 * is that this function returns the currently visible version of a row if
 * the AM supports storing multiple row versions reachable via a single index
 * entry (like heap's HOT). Whereas table_tuple_fetch_row_version() only
 * evaluates the tuple exactly at `tid`. Outside of index entry ->table tuple
 * lookups, table_tuple_fetch_row_version() is what's usually needed.
 */
static inline bool
table_index_fetch_tuple(struct IndexFetchTableData *scan,
						ItemPointer tid,
						Snapshot snapshot,
						TupleTableSlot *slot,
						bool *call_again, bool *all_dead)
{
	/*
	 * We don't expect direct calls to table_index_fetch_tuple with valid
	 * CheckXidAlive for catalog or regular tables.  See detailed comments in
	 * xact.c where these variables are declared.
	 */
	if (unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan))
		elog(ERROR, "unexpected table_index_fetch_tuple call during logical decoding");

	return scan->rel->rd_tableam->index_fetch_tuple(scan, tid, snapshot,
													slot, call_again,
													all_dead);
}

/*
 * This is a convenience wrapper around table_index_fetch_tuple() which
 * returns whether there are table tuple items corresponding to an index
 * entry.  This likely is only useful to verify if there's a conflict in a
 * unique index.
 */
extern bool table_index_fetch_tuple_check(Relation rel,
										  ItemPointer tid,
										  Snapshot snapshot,
										  bool *all_dead);
```
## Detailed Description
This function is part of PostgreSQL's table access method (tableam) interface that provides a standardized way to terminate and clean up index fetch operations. When called, it invokes the table access method's specific `index_fetch_end` function pointer to deallocate any resources that were allocated during the lifetime of the index fetch operation.

This function represents the final cleanup phase of an index fetch operation, ensuring that all allocated memory, locks, or other resources are properly released. It's the counterpart to table_index_fetch_begin and should be called when the index fetch operation is completely finished.

The function serves as a thin wrapper around the table access method's implementation, allowing different storage engines to handle resource deallocation in their own specific way while maintaining a consistent interface.

## Parameters / Member Variables
- `scan`: Pointer to IndexFetchTableData structure containing the index fetch state and associated table relation information that needs to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [IndexFetchTableData](../I/IndexFetchTableData.md) (structure type)
  - rd_tableam->index_fetch_end (table access method function pointer)
- Called from (representative examples):
  - [index_endscan](../i/index_endscan.md) (src/backend/access/index/indexam.c:386)
  - [table_index_fetch_tuple_check](table_index_fetch_tuple_check.md) (src/backend/access/table/tableam.c:223)
  - [unique_key_recheck](../u/unique_key_recheck.md) (src/backend/commands/constraint.c:120, 123)

## Notes and Other Information
- This is an inline function defined in the tableam.h header file
- Part of the table access method abstraction layer introduced to support pluggable storage engines
- The actual implementation is delegated to the specific table access method via a function pointer
- Should be paired with a corresponding table_index_fetch_begin call
- Critical for preventing resource leaks in index scanning operations

## Simplified Source

```c
static inline void table_index_fetch_end(struct IndexFetchTableData *scan) {
    // Delegate to the table access method's cleanup function
    scan->rel->rd_tableam->index_fetch_end(scan);
}
```