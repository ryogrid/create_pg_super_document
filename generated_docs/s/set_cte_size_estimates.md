# set_cte_size_estimates

## Location
[src/backend/optimizer/path/costsize.c:5967-6004](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5967-L6004)

## Overview
Sets the size estimates for a base relation that represents a Common Table Expression (CTE) reference, handling both regular and recursive CTE cases.

## Definition

```c
void
set_cte_size_estimates(PlannerInfo *root, RelOptInfo *rel, double cte_rows)
```
## Detailed Description
This function estimates cardinality for relations that reference Common Table Expressions (WITH clauses). It handles two distinct scenarios: regular CTE references and recursive CTE self-references. For regular CTEs, it simply uses the provided CTE row estimate. For recursive CTEs with self-references, it applies a configurable multiplier () to account for the iterative nature of recursive queries.

The function distinguishes between these cases using the  flag in the range table entry. For self-referencing CTEs, it assumes the average worktable size will be larger than the base (non-recursive) term by the specified factor, then clamps the result to ensure a reasonable estimate. This approach acknowledges that recursive query fan-out varies significantly depending on data characteristics.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and global information
- : RelOptInfo structure representing the CTE relation being sized, must be a base relation with CTE RTE
- : Estimated number of rows returned by the CTE or its non-recursive term

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - RTE_CTE
  - [clamp_row_est](../c/clamp_row_est.md)
  - [set_baserel_size_estimates](set_baserel_size_estimates.md)
  - recursive_worktable_factor (configuration parameter)
- Called from (representative examples):
  - [set_cte_pathlist](set_cte_pathlist.md)
  - [set_worktable_pathlist](set_worktable_pathlist.md)

## Notes and Other Information
- Handles both regular and recursive CTE estimation with different strategies
- Uses  GUC parameter for recursive CTE size multiplication
- Applies  to ensure recursive estimates remain within reasonable bounds
- The recursive multiplier is adjustable to accommodate different query fan-out patterns
- Should only be applied to base relations with RTE_CTE range table entry type
- Self-referencing logic specifically addresses the unique sizing challenges of recursive queries

## Simplified Source

This function handles CTE size estimation with special logic for recursive cases:

```c
void set_cte_size_estimates(PlannerInfo *root, RelOptInfo *rel, double cte_rows)
{
    RangeTblEntry *rte;

    // Validate this is a CTE relation
    Assert(rel->relid > 0);
    rte = planner_rt_fetch(rel->relid, root);
    Assert(rte->rtekind == RTE_CTE);

    // Set tuple count based on CTE type
    if (rte->self_reference)
    {
        // For recursive CTEs: apply multiplier to account for iteration
        rel->tuples = clamp_row_est(recursive_worktable_factor * cte_rows);
    }
    else
    {
        // For regular CTEs: use provided estimate directly
        rel->tuples = cte_rows;
    }

    // Calculate remaining size estimates (selectivity, width, etc.)
    set_baserel_size_estimates(root, rel);
}
```

**Key simplifications made:**
- Condensed the explanatory comments while preserving the key distinction
- Maintained the recursive vs non-recursive logic clearly
- Preserved essential validation and delegation to base estimation function
- Reduced from ~38 lines to ~20 lines while keeping all functionality