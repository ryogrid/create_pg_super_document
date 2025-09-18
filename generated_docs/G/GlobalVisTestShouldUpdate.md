# GlobalVisTestShouldUpdate

## Location
[src/backend/storage/ipc/procarray.c:4146-4164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4146-L4164)

## Overview
Determines whether it's worth updating the global visibility state boundaries based on current transaction activity.

## Definition


## Detailed Description
This function implements a heuristic to decide whether recomputing the global visibility horizons would be beneficial. Since determining xmin horizons is somewhat expensive, the function avoids repeated calculations when there's low likelihood of benefit.

The heuristic checks if RecentXmin has changed since the last update. If the oldest currently running transaction hasn't finished, recomputing the horizon is unlikely to be useful. Additionally, if the maybe_needed and definitely_needed boundaries are the same, refreshing boundaries won't provide benefit.

## Parameters / Member Variables
- : Pointer to the GlobalVisState structure containing current visibility boundaries

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - FullTransactionIdFollowsOrEquals
- Global variables referenced:
  - ComputeXidHorizonsResultLastXmin
  - RecentXmin
- Called from:
  - [GlobalVisTestIsRemovableFullXid](GlobalVisTestIsRemovableFullXid.md)

## Notes and Other Information
- This is a static function, only visible within procarray.c
- Returns true if horizons should be updated, false otherwise
- The function performs three checks: uninitialized state, boundary convergence, and xmin changes
- Helps optimize performance by avoiding unnecessary horizon recomputations
- Critical for maintaining efficiency in visibility testing operations