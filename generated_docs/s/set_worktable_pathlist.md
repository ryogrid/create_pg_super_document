# set_worktable_pathlist

## Location
src/backend/optimizer/path/allpaths.c: 2993 - 3051

## Overview
Builds the access path for a self-reference CTE (Common Table Expression) RTE, creating a single WorktableScan path for recursive CTE references.

## Definition
```c
static void
set_worktable_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```

## Detailed Description
This function constructs the pathlist for worktable relations, which are used in PostgreSQL's implementation of recursive Common Table Expressions (CTEs). A worktable represents the recursive reference within a CTE that refers back to itself during recursive query execution.

The function navigates up the planner hierarchy to find the non-recursive term's path from the plan level that processes the recursive UNION (one level below where the CTE originates). It then uses this path information to estimate the size of the worktable relation and creates a WorktableScan path. The function includes error checking to ensure the CTE structure is valid and the required paths exist.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global information about the current query planning context
- `rel`: RelOptInfo structure representing the worktable relation for which paths are being generated  
- `rte`: RangeTblEntry that describes this worktable CTE in the range table

## Dependencies
- Functions called/Symbols referenced:
  - [set_cte_size_estimates](set_cte_size_estimates.md)
  - [add_path](../a/add_path.md)
  - [create_worktablescan_path](../c/create_worktablescan_path.md)
- Called from (representative examples):
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- Specifically designed for self-referencing recursive CTEs
- Navigates the planner hierarchy using ctelevelsup to find the correct non-recursive path
- No separate size estimation phase needed since join-qual-parameterized paths are not supported for CTEs
- Includes comprehensive error checking for malformed CTE structures
- Handles LATERAL parameterization through lateral_relids field
- The worktable acts as a temporary storage mechanism during recursive CTE evaluation