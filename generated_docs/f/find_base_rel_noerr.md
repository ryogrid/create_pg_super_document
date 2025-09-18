# find_base_rel_noerr

## Location
[src/backend/optimizer/util/relnode.c:436-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L436-L453)

## Overview
Safely retrieves an existing RelOptInfo for a base or other relation from the planner's relation array, returning NULL if the relation doesn't exist instead of raising an error.

## Definition
```c
RelOptInfo *find_base_rel_noerr(PlannerInfo *root, int relid)
```

## Detailed Description
This function provides a non-error variant of find_base_rel for cases where the caller needs to check whether a relation exists without triggering an error. It performs the same bounds checking using unsigned comparison to prevent negative array access, but returns NULL when the relation is not found rather than raising an ERROR.

This makes it suitable for tentative lookups where the absence of a relation is a valid condition that should be handled gracefully by the caller.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the simple_rel_array
- `relid`: Range table index (1-based) of the relation to find

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a simple array access function)
- Data structures used:
  - RelOptInfo (return type)  
  - [PlannerInfo](../P/PlannerInfo.md) (contains simple_rel_array)
- Called from (representative examples):
  - all_rows_selectable (src/backend/utils/adt/selfuncs.c:5620)

## Notes and Other Information
- Returns NULL if relation does not exist, making it safe for tentative lookups
- Complement to find_base_rel which raises ERROR on missing relations
- Uses the same unsigned comparison technique for efficient bounds checking
- Much less frequently used than find_base_rel, primarily in statistical functions
- Suitable for cases where relation existence is uncertain or optional
- Part of the core relation access API providing both error and non-error variants