# heap_prepare_freeze_tuple

## Location
src/backend/access/heap/heapam.c: 7009 - 7282

## Overview
Analyzes a tuple's transaction ID fields (xmin, xmax, xvac) to determine if freezing is needed and prepares a freeze plan that can be executed to freeze the tuple while maintaining MVCC consistency.

## Definition


## Detailed Description
heap_prepare_freeze_tuple is a core component of PostgreSQL's tuple freezing mechanism, responsible for analyzing tuple headers and preparing freeze plans during VACUUM operations. The function examines all transaction ID fields in a tuple (xmin, xmax, xvac) against various age-based cutoffs to determine what freezing actions are needed.

The function implements sophisticated logic to:
1. Validate transaction IDs against corruption scenarios
2. Determine which fields need freezing based on cutoff thresholds
3. Handle complex MultiXactId scenarios through FreezeMultiXactId
4. Prepare detailed freeze plans with appropriate infomask modifications
5. Track whether tuples become totally frozen after processing
6. Coordinate page-level freezing requirements

The function returns true if a freeze plan was prepared, false if no action is needed. It ensures that the FreezeLimit and MultiXactCutoff postconditions are never violated while optimizing for performance by avoiding unnecessary work.

## Parameters
- : Pointer to the tuple header to analyze for freezing
- : Structure containing various vacuum cutoff thresholds (FreezeLimit, OldestXmin, etc.)
- : Input/output structure managing page-level freezing state and requirements
- : Output structure containing the prepared freeze plan for this tuple
- : Output parameter indicating if tuple will be completely frozen after plan execution

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderGetXmin
  - HeapTupleHeaderGetXvac
  - TransactionIdIsNormal
  - TransactionIdPrecedes
  - TransactionIdPrecedesOrEquals
  - FreezeMultiXactId
  - GetMultiXactIdHintBits
  - heap_tuple_should_freeze
  - MultiXactIdPrecedes
  - MultiXactIdIsValid
  - HEAP_XMAX_IS_MULTI
  - HEAP_XMAX_IS_LOCKED_ONLY
  - HEAP_MOVED_OFF
- Called from:
  - heap_freeze_tuple
  - heap_prune_record_unchanged_lp_normal
  - HeapScanIsValid (via header inclusion)

## Notes and Other Information
- **Return Value**: Returns true if any freeze plan was prepared, false if tuple needs no changes
- **Side Effects**: May allocate new MultiXactIds when processing complex xmax values
- **Freeze Plan Structure**: The output frz structure contains detailed instructions for tuple modification including new XID values and infomask changes
- **Page-Level Coordination**: Works with pagefrz to manage page-level freezing requirements and track various cutoff thresholds
- **Corruption Detection**: Includes extensive validation to detect and report data corruption scenarios
- **MultiXact Handling**: Delegates complex MultiXactId processing to FreezeMultiXactId while managing the integration of results
- **Total Freezing**: Tracks whether tuples become completely frozen (no remaining XIDs/MXIDs needing future processing)
- **Performance Optimization**: Designed to minimize unnecessary work while ensuring all freezing postconditions are met
- **MVCC Compliance**: Maintains MVCC semantics by carefully validating transaction states before freezing
- **Buffer Locking**: Caller must hold exclusive lock on shared buffers containing the tuple