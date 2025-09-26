# beginmerge

## Location
[src/backend/utils/sort/tuplesort.c:2292-2319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2292-L2319)

## Overview
Initializes the merge phase by filling the merge heap with the first tuple from each active input tape during a merge pass in the tuplesort algorithm.

## Definition

```c
static void
beginmerge(Tuplesortstate *state)
```
## Detailed Description
The beginmerge function prepares for a merge pass by reading the first tuple from each active input tape and inserting them into a heap-based priority queue. This function is called at the start of merge operations to establish the initial state where the merge heap contains one tuple from each input run that will be merged. The function determines the number of active tapes based on the minimum of available input tapes and input runs, then iterates through each active tape to read and heap-insert the first available tuple.

## Parameters / Member Variables
- : Pointer to the Tuplesortstate structure containing the current state of the tuple sorting operation, including tape information and heap management data

## Dependencies
- Functions called/Symbols referenced:
  - [Tuplesortstate](../T/Tuplesortstate.md) (state structure)
  - SortTuple (tuple structure for sorting)
  - [mergereadnext](../m/mergereadnext.md) (reads next tuple from tape)
  - [tuplesort_heap_insert](../t/tuplesort_heap_insert.md) (inserts tuple into merge heap)
- Called from (representative examples):
  - [mergeruns](../m/mergeruns.md)
  - [mergeonerun](../m/mergeonerun.md)

## Notes and Other Information
- The function assumes the merge heap is initially empty (verified by Assert)
- Only processes active tapes up to the minimum of nInputTapes and nInputRuns
- Each tuple inserted into the heap is tagged with its source tape index for tracking during merge
- This is a static function internal to tuplesort.c, not exposed in the public API