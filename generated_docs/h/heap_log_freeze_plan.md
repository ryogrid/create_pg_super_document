# heap_log_freeze_plan

## Location
[src/backend/access/heap/pruneheap.c:1978-2052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L1978-L2052)

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

## Simplified Source

```c
static int heap_log_freeze_plan(HeapTupleFreeze *tuples, int ntuples,
                              xlhp_freeze_plan *plans_out,
                              OffsetNumber *offsets_out) {
    int nplans = 0;

    // Sort freeze plans to enable deduplication
    qsort(tuples, ntuples, sizeof(HeapTupleFreeze), heap_log_freeze_cmp);

    // Process each tuple to build deduplicated plans
    for (int i = 0; i < ntuples; i++) {
        HeapTupleFreeze *frz = tuples + i;

        if (i == 0) {
            // Start first canonical freeze plan
            heap_log_freeze_new_plan(plans_out, frz);
            nplans++;
        } else if (heap_log_freeze_eq(plans_out, frz)) {
            // Tuple matches current plan - add to it
            Assert(offsets_out[i - 1] < frz->offset);
            plans_out->ntuples++;
        } else {
            // Tuple needs different plan - start new one
            plans_out++;
            heap_log_freeze_new_plan(plans_out, frz);
            nplans++;
        }

        // Record offset number for this tuple
        // REDO routine needs offsets grouped by freeze plan
        offsets_out[i] = frz->offset;
    }

    Assert(nplans > 0 && nplans <= ntuples);
    return nplans;
}
```