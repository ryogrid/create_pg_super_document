# _bt_dedup_finish_pending

## Location
[src/backend/access/nbtree/nbtdedup.c:555-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L555-L647)

## Overview
Finalizes a pending posting list tuple and adds it to the page, either as the original base tuple or as a new posting list tuple with merged heap TIDs.

## Definition

```c
Size
_bt_dedup_finish_pending(Page newpage, BTDedupState state)
```
## Detailed Description
This function completes the deduplication process for a pending posting list by deciding whether to create a new posting list tuple or use the original base tuple unchanged. The decision depends on whether any duplicates were found and merged.

The function handles two scenarios:
1. **No duplicates found** (nitems == 1): Adds the original base tuple unchanged to the page
2. **Duplicates found** (nitems > 1): Creates a new posting list tuple using  and adds it to the page

When creating posting lists, the function tracks the operation in the deduplication intervals array and calculates space savings achieved by replacing multiple physical tuples with a single posting list tuple. This space calculation includes line pointer overhead for accurate accounting.

## Parameters / Member Variables
- : Target page where the finalized tuple will be added
- : Deduplication state containing the pending posting list data and metadata

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the highest offset number on the page
  - : Calculates the next available offset number
  - : Calculates tuple size
  - : Validates tuple fits on page
  - : Adds tuple to the page
  - : Creates a new posting list tuple from base tuple and heap TIDs

- Called from (representative examples):
  - : Called to finalize each pending interval during deduplication
  - : Called during WAL replay of deduplication operations

## Notes and Other Information
- Returns the space saved by deduplication (including line pointer overhead), or 0 if no deduplication occurred
- The function increments nintervals only when a new posting list is created, not for unchanged base tuples
- Space savings calculation accounts for the difference between original physical tuple sizes and the new posting list size
- After processing, the function resets state variables (nhtids, nitems, phystupsize) for the next pending interval
- The function validates that tuples fit within page size limits and posting list size constraints
- All space calculations use MAXALIGN for proper tuple alignment
- Error handling includes assertions and elog(ERROR) for cases that should never occur in normal operation