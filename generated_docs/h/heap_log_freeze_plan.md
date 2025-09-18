# heap_log_freeze_plan

## Location
src/backend/access/heap/pruneheap.c: 1978 - 2052

## Overview
Deduplicates tuple-based freeze plans so that each distinct set of processing steps is stored only once in XLOG_HEAP2_FREEZE_PAGE records, called during original execution of freezing for logged relations.

## Definition
```c
static int heap_log_freeze_plan(HeapTupleFreeze *tuples, int ntuples,
                               xlhp_freeze_plan *plans_out,
                               OffsetNumber *offsets_out)
```

## Detailed Description
This function optimizes WAL logging by consolidating identical freeze operations across multiple tuples into shared freeze plans. It sorts the input tuple freeze requests using `heap_log_freeze_cmp`, then iterates through them to identify groups of tuples that require the same freeze operations. Each unique freeze plan is created once and shared among all tuples that need the same processing steps.

The function maintains an array of canonical freeze plans in `plans_out` and tracks which tuples belong to each plan through the `offsets_out` array. The REDO routine during recovery relies on the offset numbers being grouped by freeze plan with ascending order within each group.

## Parameters / Member Variables
- `tuples`: Array of HeapTupleFreeze structures containing freeze requests for individual tuples
- `ntuples`: Number of tuple freeze requests in the input array
- `plans_out`: Output array where deduplicated freeze plans will be stored
- `offsets_out`: Output array where page offset numbers will be stored, grouped by freeze plan

## Dependencies
- Functions called/Symbols referenced:
  - qsort
  - [heap_log_freeze_cmp](heap_log_freeze_cmp.md)
  - [heap_log_freeze_new_plan](heap_log_freeze_new_plan.md)
  - [heap_log_freeze_eq](heap_log_freeze_eq.md)
  - [HeapTupleFreeze](../H/HeapTupleFreeze.md)
  - [xlhp_freeze_plan](../x/xlhp_freeze_plan.md)
- Called from (representative examples):
  - [log_heap_prune_and_freeze](../l/log_heap_prune_and_freeze.md)

## Notes and Other Information
The function ensures that freeze plans are stored in a canonical form to minimize WAL record size and improve recovery performance. The sorting and deduplication process is critical for efficient WAL logging of freeze operations on multiple tuples within the same page. The output maintains strict ordering requirements that the REDO routine depends on during crash recovery.