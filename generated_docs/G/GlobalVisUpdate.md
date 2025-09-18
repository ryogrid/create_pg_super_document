# GlobalVisUpdate

## Location
[src/backend/storage/ipc/procarray.c:4204-4220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4204-L4220)

## Overview
Updates global visibility boundaries for all relation types by triggering a complete horizon recomputation.

## Definition


## Detailed Description
This function serves as a simple wrapper that triggers a complete update of global visibility boundaries for shared, catalog, data, and temporary relations. It calls ComputeXidHorizons() which performs the actual horizon calculation and applies the results to the global visibility states as a side effect.

The function is used when the system determines that the current visibility boundaries may be stale and need refreshing based on current transaction activity.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ComputeXidHorizons
- Types used:
  - [ComputeXidHorizonsResult](../C/ComputeXidHorizonsResult.md)
- Called from:
  - [GlobalVisTestIsRemovableFullXid](GlobalVisTestIsRemovableFullXid.md)

## Notes and Other Information
- This is a static function, only visible within procarray.c
- Very simple wrapper function that delegates all work to ComputeXidHorizons()
- ComputeXidHorizons() updates the global visibility states as a side effect
- The horizons variable is local and not used after ComputeXidHorizons() returns
- Part of the global visibility infrastructure for maintaining MVCC correctness
- Called when heuristics determine that boundary updates would be beneficial