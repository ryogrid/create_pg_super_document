# tuplesort_used_bound

## Location
[src/backend/utils/sort/tuplesort.c:891-901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L891-L901)

## Overview
Allows callers to determine if the sort state was able to successfully use a bound optimization.

## Definition
```c
bool tuplesort_used_bound(Tuplesortstate *state)
```

## Detailed Description
This function provides a way for callers to query whether the tuplesort system was actually able to utilize a bound that was previously set via tuplesort_set_bound. While tuplesort_set_bound provides a hint about the desired number of result tuples, the sort system may not always be able to take advantage of this information due to various constraints or conditions.

The function simply returns the value of the boundUsed flag from the sort state, which is set internally by the tuplesort system when it determines that bounded sort optimizations were successfully applied.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure representing the sort operation

## Dependencies
- Functions called/Symbols referenced:
  - Tuplesortstate (sort state structure)

- Called from (representative examples):
  - ExecIncrementalSort (incremental sort node execution to check optimization usage)

## Notes and Other Information
- This is a query function that does not modify the sort state
- Returns the internal boundUsed flag which indicates actual usage, not just the presence of a bound hint
- Useful for performance analysis and debugging to understand when bounded sort optimizations are effective
- The boundUsed flag is set internally by the tuplesort system based on various conditions and constraints
- Typically called after sort completion to determine if the optimization was beneficial