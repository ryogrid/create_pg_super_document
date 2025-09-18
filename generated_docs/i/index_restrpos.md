# index_restrpos

## Location
src/backend/access/index/indexam.c: 432 - 452

## Overview
The index_restrpos function restores an index scan to a previously marked position, complementing index_markpos to enable backtracking during index scan operations.

## Definition
```c
void index_restrpos(IndexScanDesc scan)
```

## Detailed Description
index_restrpos restores the internal scan state of an index access method to a position previously marked with index_markpos. This function is crucial for implementing backtracking functionality in scan operations. It includes important constraints and safety measures: it requires MVCC snapshots for proper operation with HOT (Heap-Only Tuples) chains, and it resets heap fetch resources and scan continuation flags to ensure consistent state.

The function performs validation to ensure the scan uses an MVCC snapshot, which is necessary for correct operation with HOT chains. It also resets various scan state flags and delegates the actual position restoration to the access method-specific amrestrpos routine.

## Parameters / Member Variables
- `scan`: IndexScanDesc - The index scan descriptor to restore to the previously marked position

## Dependencies
- Functions called/Symbols referenced:
  - IsMVCCSnapshot (validates snapshot type)
  - SCAN_CHECKS (validation macro for scan descriptor)
  - CHECK_SCAN_PROCEDURE (validation macro for amrestrpos availability)
  - table_index_fetch_reset (resets heap fetch resources)
  - amrestrpos (access method-specific position restoration routine)
- Called from (representative examples):
  - [ExecIndexRestrPos](../E/ExecIndexRestrPos.md)
  - [ExecIndexOnlyRestrPos](../E/ExecIndexOnlyRestrPos.md)

## Notes and Other Information
- Requires MVCC snapshots to work correctly with HOT chains - this ensures at most one returnable tuple per HOT chain
- Currently used primarily by merge-join operations, which effectively limits its use to MVCC snapshots
- Resets kill_prior_tuple and xs_heap_continue flags for safety and consistency
- Must be preceded by a call to index_markpos to establish the restore point
- The function only restores the internal index AM scan state, not the complete executor state
- Located in src/backend/access/index/indexam.c:432-452
- Includes detailed comments about HOT chain handling and MVCC snapshot requirements