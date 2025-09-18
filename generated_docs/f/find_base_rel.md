# find_base_rel

## Location
[src/backend/optimizer/util/relnode.c:414-435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L414-L435)

## Overview
Retrieves an existing RelOptInfo for a base or other relation from the planner's relation array, with error handling for missing relations.

## Definition
```c
RelOptInfo *find_base_rel(PlannerInfo *root, int relid)
```

## Detailed Description
This function provides safe access to RelOptInfo structures stored in the simple_rel_array. It performs bounds checking using unsigned comparison to prevent negative array access and verifies that the requested relation actually exists. If the relation is not found, it raises an ERROR rather than returning NULL, making it suitable for cases where the relation is expected to exist.

The function uses an unsigned comparison trick `(uint32) relid < (uint32) root->simple_rel_array_size` to simultaneously check for both negative values and out-of-bounds access in a single comparison.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the simple_rel_array
- `relid`: Range table index (1-based) of the relation to find

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting when relation not found)
- Data structures used:
  - RelOptInfo (return type)
  - [PlannerInfo](../P/PlannerInfo.md) (contains simple_rel_array)
- Called from (representative examples):
  - [set_base_rel_consider_startup](../s/set_base_rel_consider_startup.md) (src/backend/optimizer/path/allpaths.c:272)
  - [make_rel_from_joinlist](../m/make_rel_from_joinlist.md) (src/backend/optimizer/path/allpaths.c:3337)
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md) (src/backend/optimizer/path/clausesel.c:922)
  - [join_is_removable](../j/join_is_removable.md) (src/backend/optimizer/plan/analyzejoins.c:188)
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1839)

## Notes and Other Information
- Raises ERROR if relation does not exist, making it unsuitable for tentative lookups
- For cases where missing relations should be handled gracefully, use find_base_rel_noerr instead
- The unsigned comparison technique efficiently handles both bounds checking and negative value detection
- Widely used throughout the optimizer when the relation is expected to exist
- Part of the core relation access API alongside build_simple_rel and find_base_rel_noerr