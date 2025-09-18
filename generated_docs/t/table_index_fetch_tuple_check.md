# table_index_fetch_tuple_check

## Location
src/backend/access/table/tableam.c: 209 - 235

## Overview
Checks whether a tuple referenced by an index entry exists and is visible according to the given snapshot, primarily used for uniqueness validation.

## Definition
```c
bool table_index_fetch_tuple_check(Relation rel, ItemPointer tid, Snapshot snapshot, bool *all_dead)
```

## Detailed Description
This function performs a simplified index fetch operation to verify tuple existence and visibility. It creates a temporary table slot, initiates an index fetch scan, performs the tuple lookup, and immediately cleans up resources. The function is optimized for checking tuple validity rather than full tuple retrieval. It supports storage engines that store multiple row versions reachable via a single index entry (like PostgreSQL's HOT - Heap Only Tuples). The tid parameter may be modified during execution if the access method supports this optimization.

## Parameters / Member Variables
- `rel`: The Relation object representing the table being checked
- `tid`: ItemPointer to the tuple's location (may be modified for HOT chains)
- `snapshot`: Snapshot defining tuple visibility rules for the check
- `all_dead`: Output parameter indicating whether all tuple versions are dead

## Dependencies
- Functions called/Symbols referenced:
  - table_slot_create
  - table_index_fetch_begin
  - table_index_fetch_tuple
  - table_index_fetch_end
  - ExecDropSingleTupleTableSlot
  - IndexFetchTableData
- Called from (representative examples):
  - _bt_check_unique (B-tree uniqueness checking)
  - table_index_fetch_tuple (inline helper in tableam.h)

## Notes and Other Information
- Designed for performance efficiency in uniqueness checking scenarios where full tuple data is not needed
- Handles cleanup automatically, making it safe for one-off tuple existence checks
- The tid parameter modification supports HOT chain following for heap access methods
- Part of PostgreSQL's table access method abstraction layer for storage engine independence
- Primary use case is B-tree unique index constraint validation during insertions