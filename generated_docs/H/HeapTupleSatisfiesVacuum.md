# HeapTupleSatisfiesVacuum

## Location
src/backend/access/heap/heapam_visibility.c: 1162 - 1195

## Overview
HeapTupleSatisfiesVacuum determines the vacuum status of heap tuples by checking if they can be safely removed by VACUUM operations, serving as a wrapper around HeapTupleSatisfiesVacuumHorizon with additional oldest transaction boundary checking.

## Definition


## Detailed Description
This function determines the status of tuples for VACUUM purposes by answering the fundamental question: "Can this tuple be safely removed by VACUUM?" The main concern is whether a tuple is potentially visible to any currently running transaction.

The function serves as a higher-level interface that:
1. Calls HeapTupleSatisfiesVacuumHorizon to get the base vacuum status
2. Applies the OldestXmin cutoff logic for recently dead tuples
3. Upgrades HEAPTUPLE_RECENTLY_DEAD to HEAPTUPLE_DEAD when appropriate

The OldestXmin parameter represents a cutoff transaction ID obtained from GetOldestNonRemovableTransactionId(). Tuples deleted by transactions with XIDs >= OldestXmin are deemed "recently dead" because they might still be visible to some open transaction, preventing their removal even if the deleting transaction has committed.

## Parameters / Member Variables
- : The heap tuple to evaluate for vacuum status, containing tuple data and metadata
- : Cutoff transaction ID below which deleted tuples can be considered truly dead and removable
- : The buffer containing the tuple, passed through to HeapTupleSatisfiesVacuumHorizon for potential hint bit setting

## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleSatisfiesVacuumHorizon](HeapTupleSatisfiesVacuumHorizon.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
- Called from (representative examples):
  - [HeapCheckForSerializableConflictOut](HeapCheckForSerializableConflictOut.md)
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md)
  - [heapam_scan_analyze_next_tuple](../h/heapam_scan_analyze_next_tuple.md)
  - [heapam_index_build_range_scan](../h/heapam_index_build_range_scan.md)
  - [lazy_scan_noprune](../l/lazy_scan_noprune.md)
  - [heap_page_is_all_visible](../h/heap_page_is_all_visible.md)

## Notes and Other Information
The function returns HTSV_Result values that indicate vacuum status:
- HEAPTUPLE_LIVE: Tuple is visible to some transaction, cannot be removed
- HEAPTUPLE_RECENTLY_DEAD: Tuple was deleted but might still be visible to some transaction
- HEAPTUPLE_DEAD: Tuple is not visible to any transaction and can be safely removed
- Other status values as determined by HeapTupleSatisfiesVacuumHorizon

The key logic upgrade happens when a tuple is initially marked as HEAPTUPLE_RECENTLY_DEAD by HeapTupleSatisfiesVacuumHorizon, but the dead_after transaction ID precedes OldestXmin, indicating that all transactions that could have seen the tuple are now complete. In this case, the status is upgraded to HEAPTUPLE_DEAD, allowing VACUUM to safely remove the tuple.

This two-level approach (base horizon checking plus oldest transaction cutoff) provides an efficient way to determine vacuum eligibility while ensuring transaction isolation properties are maintained.