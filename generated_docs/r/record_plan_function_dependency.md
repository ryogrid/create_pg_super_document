# record_plan_function_dependency

## Location
src/backend/optimizer/plan/setrefs.c: 3472 - 3511

## Overview
Records a dependency of the current query plan on a specific function to enable proper plan invalidation when the function is modified.

## Definition
```c
void
record_plan_function_dependency(PlannerInfo *root, Oid funcid)
```

## Detailed Description
This function is part of PostgreSQL's plan invalidation mechanism, which ensures that cached query plans are invalidated when the database objects they depend on are modified. When a query plan references a function (either directly in expressions or indirectly through inlining), this dependency must be tracked so that if the function is later altered or dropped, any cached plans using it can be invalidated and replanned.

The function creates a PlanInvalItem entry that identifies the function using the PROCOID syscache and its hash value. This information is stored in the global planner state (root->glob->invalItems) and will later be used by the plan caching system to determine when plans need to be invalidated.

For performance optimization, built-in functions (those with OIDs less than FirstUnpinnedObjectId) are not tracked, as they are assumed to never change in ways that would invalidate plans.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the global planner state where dependency information is stored
- `funcid`: OID of the function on which the plan depends

## Dependencies
- Functions called/Symbols referenced:
  - FirstUnpinnedObjectId (constant for built-in object threshold)
  - PlanInvalItem (structure type)
  - makeNode (node creation function)
  - GetSysCacheHashValue1 (syscache hash function)
  - lappend (list append function)
- Called from (representative examples):
  - [fix_expr_common](../f/fix_expr_common.md) (multiple calls for different expression types)
  - [inline_function](../i/inline_function.md)
  - [inline_set_returning_function](../i/inline_set_returning_function.md)

## Notes and Other Information
- This is an exported function (not static) so that function inlining code can record dependencies on functions removed from the plan tree
- Uses PROCOID syscache for tracking function dependencies, which plancache.c specifically expects
- Built-in functions are deliberately not tracked for performance reasons, based on the assumption they don't change
- Part of the broader plan invalidation system that ensures plan cache correctness
- Critical for maintaining consistency when functions are modified after plans are cached
- The dependency tracking enables automatic plan recompilation when dependent objects change