# tuplesort_set_bound

## Location
[src/backend/utils/sort/tuplesort.c:843-890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L843-L890)

## Overview
Advises the tuplesort system that at most the first N result tuples are required, enabling bounded sort optimizations.

## Definition
```c
void tuplesort_set_bound(Tuplesortstate *state, int64 bound)
```

## Detailed Description
This function provides a hint to the tuplesort system that only the first N tuples are needed from the sort result. This allows the sorting algorithm to optimize its behavior by using bounded sorting techniques, which can significantly reduce memory usage and improve performance when only a small subset of sorted results is required.

The function must be called before inserting any tuples into the sort state. It performs several validation checks to ensure proper usage and sets up the sort state for bounded operation. Importantly, this is only a hint - the tuplesort may still return more tuples than requested, and parallel leader tuplesorts will always ignore the hint.

When a bound is set, the function also disables abbreviated key optimization since bounded sorts are not effective targets for this optimization technique.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure representing the sort operation
- `bound`: Maximum number of result tuples needed (int64), limited to INT_MAX/2 to allow bound*2 computation

## Dependencies
- Functions called/Symbols referenced:
  - Tuplesortstate (sort state structure)
  - TSS_INITIAL (initial sort status constant)
  - TUPLESORT_ALLOWBOUNDED (sort option flag)
  - WORKER (macro to check if running in parallel worker)
  - LEADER (macro to check if running in parallel leader)

- Called from (representative examples):
  - switchToPresortedPrefixMode (incremental sort execution)
  - ExecIncrementalSort (incremental sort node execution)
  - ExecSort (sort node execution)

## Notes and Other Information
- Must be called before any tuples are inserted (checked via assertions)
- Requires TUPLESORT_ALLOWBOUNDED flag to be set in sort options
- Cannot be called twice on the same sort state
- Not allowed in parallel worker processes
- Parallel leaders accept but ignore the hint
- Disables abbreviated key optimization for better bounded sort performance
- Bounded sorts are primarily useful for LIMIT operations in SQL queries
- The bound is limited to INT_MAX/2 to prevent integer overflow in internal calculations