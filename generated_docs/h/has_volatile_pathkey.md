# has_volatile_pathkey

## Location
[src/backend/optimizer/plan/planner.c:3240-3284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L3240-L3284)

## Overview
Checks whether any PathKey in a given list contains an EquivalenceClass with volatile functions, returning true if found.

## Definition
```c
static bool has_volatile_pathkey(List *keys)
```

## Detailed Description
This utility function examines a list of PathKeys to determine if any of them contain volatile functions within their associated EquivalenceClasses. Volatile functions are those whose results can change between calls even with the same arguments (like random(), now(), etc.).

The function iterates through each PathKey in the input list and checks the `ec_has_volatile` flag of the PathKey's EquivalenceClass (`pk_eclass->ec_has_volatile`). If any PathKey is found to have a volatile EquivalenceClass, the function immediately returns true.

This check is important for query optimization decisions, particularly when determining whether certain optimizations like grouping aggregates are safe to apply, since volatile functions can affect the correctness of such optimizations.

## Parameters / Member Variables
- `keys`: List of PathKey structures to examine for volatile functions

## Dependencies
- Functions called/Symbols referenced:
  - [PathKey](../P/PathKey.md) (structure access)
- Called from (representative examples):
  - [adjust_group_pathkeys_for_groupagg](../a/adjust_group_pathkeys_for_groupagg.md)

## Notes and Other Information
- Returns false if the input list is empty or if no PathKeys contain volatile EquivalenceClasses
- The volatile check is performed at the EquivalenceClass level rather than individual expression level
- This function is used as a safety check before applying certain query optimizations
- Volatile functions can include system functions like random(), clock_timestamp(), txid_current(), etc.
- The presence of volatile functions typically prevents certain query rewrite optimizations