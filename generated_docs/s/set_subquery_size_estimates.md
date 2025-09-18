# set_subquery_size_estimates

## Location
src/backend/optimizer/path/costsize.c: 5795 - 5874

## Overview  
Sets size and width estimates for base relations that represent subqueries by extracting information from the completed subquery planning process.

## Definition
```c
void set_subquery_size_estimates(PlannerInfo *root, RelOptInfo *rel)
```

## Detailed Description
This function is responsible for establishing size estimates for relations that represent subqueries in the query plan. It operates after the subquery has been completely planned, allowing it to extract accurate information from the subquery's planning results.

The estimation process involves several key steps:

1. **Row Count Extraction**: Retrieves the output row count from the subquery's final relation by examining the cheapest total path. All paths for a relation should have consistent row counts, so using the cheapest path is sufficient.

2. **Column Width Estimation**: Analyzes the subquery's target list to estimate per-column widths:
   - For output columns that are simple Vars, uses the width estimates computed during subquery planning
   - For complex expressions, leaves width estimation to `set_rel_width` which will apply datatype-based defaults
   - Handles view expansion scenarios where the subquery may have more columns than visible to the outer query

3. **Final Size Calculation**: Calls `set_baserel_size_estimates` to compute final estimates including total relation size, pages, and other derived metrics.

The function includes several important safety checks and limitations:
- Validates that the relation is indeed a subquery using assertions
- Handles edge cases like set operations and empty appendrels gracefully
- Skips junk columns and columns outside the visible range

## Parameters / Member Variables
- `root`: PlannerInfo structure for the current (outer) query planning context
- `rel`: RelOptInfo representing the subquery relation whose size is being estimated

## Dependencies  
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - fetch_upper_rel
  - [find_base_rel](../f/find_base_rel.md)
  - [set_baserel_size_estimates](set_baserel_size_estimates.md)
  - Constants: RTE_SUBQUERY, UPPERREL_FINAL
- Called from (representative examples):
  - [set_subquery_pathlist](set_subquery_pathlist.md) (src/backend/optimizer/path/allpaths.c:2661)
  - [build_setop_child_paths](../b/build_setop_child_paths.md) (src/backend/optimizer/prep/prepunion.c:527)

## Notes and Other Information
- Must be called after the subquery's planning is complete and paths are available
- Sets the same fields as `set_baserel_size_estimates` for consistency with base table estimation
- Has known limitations with set operations where Vars in target lists reference the first leaf subquery incorrectly
- Handles view expansion scenarios where subqueries may have evolved since the outer query was parsed
- Gracefully handles empty appendrels due to constraint exclusion by leaving width estimates at zero for `set_rel_width` to fix
- The function assumes that all paths for the final relation have the same row count, which should be guaranteed by the planning process
- Critical for accurate cost estimation of queries involving subqueries, views, and CTEs