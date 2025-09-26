# table_index_fetch_tuple

## Location
[src/include/access/tableam.h:1242-1288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1242-L1288)

## Overview
Fetches a tuple at a specific TID as part of an index scan, performing visibility tests and handling multiple row versions reachable via a single index entry.

## Definition

```c
static inline bool table_index_fetch_tuple(struct IndexFetchTableData *scan,
					ItemPointer tid, Snapshot snapshot,
					TupleTableSlot *slot, bool *call_again, bool *all_dead)
```
## Detailed Description
This function is a core component of PostgreSQL's index scanning mechanism within the table access method (tableam) interface. It retrieves a tuple identified by a tuple ID (TID) and tests its visibility according to the provided snapshot. The function is designed to handle advanced scenarios like PostgreSQL's Heap-Only Tuple (HOT) optimization, where multiple row versions can be reached through a single index entry.

The function includes several sophisticated features:
- Visibility testing based on transaction snapshots
- Support for multiple row versions per index entry (HOT chains)
- Detection of dead tuples that can be cleaned up
- Prevention of use during logical decoding operations
- Iterator-like behavior for processing multiple versions

The key distinction from table_tuple_fetch_row_version() is that this function can return the currently visible version of a row when multiple versions exist, whereas table_tuple_fetch_row_version() only evaluates the exact tuple at the specified TID.

## Parameters / Member Variables
- `scan`: Pointer to IndexFetchTableData structure containing the index fetch state and associated table relation
- `tid`: ItemPointer identifying the specific tuple to fetch (may be modified on return for HOT chains)
- `snapshot`: Snapshot used for visibility testing to determine which tuple versions are visible
- `slot`: TupleTableSlot where the fetched tuple data will be stored
- `call_again`: Output parameter set to true if there are more tuple versions to process for the same TID
- `all_dead`: Output parameter set to true if all tuple versions are guaranteed to be dead to all backends

## Dependencies
- Functions called/Symbols referenced:
  - [IndexFetchTableData](../I/IndexFetchTableData.md) (structure type)
  - ItemPointer (tuple identifier type)
  - [Snapshot](../S/Snapshot.md) (visibility testing context)
  - [TupleTableSlot](../T/TupleTableSlot.md) (tuple storage)
  - TransactionIdIsValid (transaction validation)
  - CheckXidAlive (logical decoding check)
  - bsysscan (system scan flag)
  - rd_tableam->index_fetch_tuple (table access method function pointer)
- Called from (representative examples):
  - [index_fetch_heap](../i/index_fetch_heap.md) (src/backend/access/index/indexam.c:637)
  - [table_index_fetch_tuple_check](table_index_fetch_tuple_check.md) (src/backend/access/table/tableam.c:221)
  - [unique_key_recheck](../u/unique_key_recheck.md) (src/backend/commands/constraint.c:112)

## Notes and Other Information
- This is an inline function defined in the tableam.h header file
- Part of the table access method abstraction layer supporting pluggable storage engines
- Includes protection against unexpected calls during logical decoding operations
- The TID parameter may be modified when returning true, reflecting HOT chain traversal
- Returns true if a visible tuple was found, false otherwise
- Critical for index scan performance, especially with HOT optimization
- The call_again mechanism allows iteration through multiple tuple versions efficiently
- The all_dead flag enables index cleanup optimizations by identifying completely dead tuples