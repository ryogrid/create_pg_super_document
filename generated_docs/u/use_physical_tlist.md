# use_physical_tlist

## Location
[src/backend/optimizer/plan/createplan.c:866-1002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L866-L1002)

## Overview
Determines whether to use a physical target list (matching relation structure) instead of only including variables actually referenced by the query.

## Definition
```c
static bool use_physical_tlist(PlannerInfo *root, Path *path, int flags)
```

## Detailed Description
use_physical_tlist implements the decision logic for determining when it is beneficial to use a "physical" target list that includes all columns from a relation in their natural order, rather than a minimal target list containing only the columns actually referenced by the query. This optimization allows the executor to potentially skip tuple projection operations when the physical tuple layout matches what is needed.

The function performs extensive validation to ensure physical target lists are only used when safe and beneficial. It checks relation types, inheritance situations, system column requirements, placeholder expressions, index-only scan constraints, and sort/group column compatibility. The optimization is particularly valuable for table scans where avoiding projection can improve performance.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner context and global information like placeholder lists
- `path`: The Path node being evaluated for physical target list usage
- `flags`: Control flags affecting the decision (CP_EXACT_TLIST, CP_SMALL_TLIST, CP_LABEL_TLIST)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - [bms_nonempty_difference](../b/bms_nonempty_difference.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - CustomPath (type check)
  - BitmapHeapPath (type check)
  - [IndexPath](../I/IndexPath.md) (cast)
  - PlaceHolderInfo (type)
  - [IndexOptInfo](../I/IndexOptInfo.md) (type)
  - Various RTE constants (RTE_RELATION, RTE_SUBQUERY, etc.)
  - Various flag constants (CP_EXACT_TLIST, CP_SMALL_TLIST, CP_LABEL_TLIST)
- Called from (representative examples):
  - [create_scan_plan](../c/create_scan_plan.md)
  - [create_projection_plan](../c/create_projection_plan.md)

## Notes and Other Information
- Returns false immediately if CP_EXACT_TLIST or CP_SMALL_TLIST flags are set, as these demand specific target list formats
- Only supports base relations, subqueries, functions, table functions, VALUES, and CTE scans
- Excludes inheritance cases since Append nodes do not project
- Prevents use with CustomPath nodes due to uncertain physical tuple assumptions
- For BitmapHeapScan with empty target lists, maintains the empty list to enable potential heap page fetch skipping
- Rejects cases involving system columns or whole-row variables due to setrefs.c complexity
- Checks placeholder expression requirements to ensure proper evaluation
- For IndexOnlyScan, validates that all index columns are returnable
- When CP_LABEL_TLIST is specified, ensures sort/group columns are simple Vars without conflicts
- Located at src/backend/optimizer/plan/createplan.c:866-1002