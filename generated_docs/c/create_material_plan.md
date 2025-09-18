# create_material_plan

## Location
src/backend/optimizer/plan/createplan.c: 1639 - 1666

## Overview
Creates a Material plan node that materializes (stores) the output of its child plan in memory or on disk, allowing multiple passes over the same result set without re-executing the child plan.

## Definition
```c
static Material *
create_material_plan(PlannerInfo *root, MaterialPath *best_path, int flags)
```

## Detailed Description
The `create_material_plan` function creates a Material execution plan node from a MaterialPath. The Material node acts as a caching layer that stores the complete output of its child plan, making it available for multiple scans without re-executing the underlying operations. This is particularly useful in scenarios where:

1. The child plan is expensive to execute and needs to be scanned multiple times
2. Hash joins need to build hash tables from the inner relation
3. Nested loops need to rescan the inner relation multiple times
4. Merge joins require mark/restore functionality

The Material node reads all tuples from its child plan on first execution and stores them. Subsequent requests for data are served from this materialized copy. The function specifically requests a smaller target list (CP_SMALL_TLIST) from the child plan to minimize memory usage, as the Material node doesn't perform any projection itself.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: MaterialPath representing the path that requires materialization of its subpath
- `flags`: Control flags that affect target list handling, passed through to the child plan with CP_SMALL_TLIST added

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md) (creates the child plan that will be materialized)
  - [make_material](../m/make_material.md) (creates the Material plan node)
  - [copy_generic_path_info](copy_generic_path_info.md) (copies common path information to the plan)
  - CP_SMALL_TLIST (flag to request minimal target list from child)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md) (main recursive plan creation function)

## Notes and Other Information
- Material nodes consume memory proportional to the size of their child's output
- Large result sets may spill to disk if they don't fit in available memory (work_mem)
- The Material node doesn't perform any projection or filtering - it passes through the child's target list unchanged
- Commonly inserted by the optimizer for inner sides of nested loop joins and hash joins
- Essential for plans that need mark/restore capability or multiple scans of the same data
- The CP_SMALL_TLIST flag ensures minimal memory overhead by requesting only necessary columns from the child plan