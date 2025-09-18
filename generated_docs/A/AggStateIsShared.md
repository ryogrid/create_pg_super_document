# AggStateIsShared

## Location
src/backend/executor/nodeAgg.c: 4615 - 4653

## Overview
Determines whether an aggregate's transition state is shared across multiple aggregates, helping aggregate functions decide if they can safely modify their transition state.

## Definition
```c
bool AggStateIsShared(FunctionCallInfo fcinfo)
```

## Detailed Description
AggStateIsShared is a utility function that helps aggregate support functions determine whether their transition state is shared with other aggregates. This information is crucial for aggregate functions to decide whether they can safely modify their transition state or input parameters.

The function examines the current aggregate execution context and checks the sharing status through two different paths:
1. When called from a final function, it checks the `curperagg` (current per-aggregate state)
2. When called from a transition function, it checks the `curpertrans` (current per-transition state)

The function returns true as a conservative default when not called as an aggregate support function or when used as a window function, effectively telling the caller "you'd better not scribble on your input".

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo structure containing the function call context and aggregate state information

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - [AggState](AggState.md) (aggregate execution state structure)
  - [AggStatePerAgg](AggStatePerAgg.md) (per-aggregate state structure)
  - [AggStatePerTrans](AggStatePerTrans.md) (per-transition state structure)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (function call information structure)
- Called from (representative examples):
  - [ordered_set_startup](../o/ordered_set_startup.md) (in orderedsetaggs.c)
  - AGG_CONTEXT_WINDOW (referenced in include/fmgr.h)

## Notes and Other Information
- Returns true (conservative answer) when not called as an aggregate support function
- Returns true for window functions as a safety measure, since modifying transition state in window context is problematic
- The sharing status is determined by checking the `aggshared` field in the relevant state structures
- Used by aggregate functions to determine if they can safely modify their inputs or transition state
- The behavior for window functions might be refined in future PostgreSQL versions
- Provides a defensive approach by defaulting to "shared" when in doubt