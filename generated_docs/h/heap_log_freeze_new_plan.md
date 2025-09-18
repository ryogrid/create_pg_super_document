# heap_log_freeze_new_plan

## Location
src/backend/access/heap/pruneheap.c: 1958 - 1977

## Overview
Initializes a new freeze plan structure with freeze operation parameters from a HeapTupleFreeze request for WAL logging optimization.

## Definition


## Detailed Description
This inline function creates a new freeze plan by copying the essential freeze operation parameters from a HeapTupleFreeze structure into an xlhp_freeze_plan structure. The function initializes a canonical freeze plan that can be shared among multiple tuples requiring identical freeze operations, thereby optimizing WAL logging by reducing redundant freeze plan records.

The function copies four key fields that define the freeze operation: the maximum transaction ID (xmax), the tuple's information mask bits (t_infomask and t_infomask2), and the freeze flags (frzflags). The ntuples field is initialized to 1, indicating that initially one tuple uses this plan, but this count can be incremented as more tuples are found to share the same freeze operation parameters.

This function is part of the freeze plan consolidation mechanism that reduces WAL volume during vacuum operations by grouping similar freeze operations together.

## Parameters / Member Variables
- : Pointer to the xlhp_freeze_plan structure to initialize
- : Pointer to the HeapTupleFreeze structure containing the source freeze parameters

## Dependencies
- Functions called/Symbols referenced:
  - xlhp_freeze_plan (struct type)
  - HeapTupleFreeze (struct type)
- Called from (representative examples):
  - heap_log_freeze_plan

## Notes and Other Information
- This is a static inline function for optimal performance during vacuum operations
- Initializes ntuples to 1, which can be incremented as more tuples share the same plan
- Part of the WAL logging optimization system for freeze operations
- Works in conjunction with heap_log_freeze_eq to identify when existing plans can be reused
- Critical for reducing WAL volume by consolidating equivalent freeze operations into shared plans
- The freeze plan will be used during WAL replay (REDO) to apply the freeze operation to all associated tuples