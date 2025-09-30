# set_subquery_size_estimates

## Location
[src/backend/optimizer/path/costsize.c:5795-5874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5795-L5874)

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
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
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

## Simplified Source

```c
void
set_subquery_size_estimates(PlannerInfo *root, RelOptInfo *rel)
{
    PlannerInfo *subroot = rel->subroot;
    RelOptInfo *sub_final_rel;
    ListCell *lc;

    // Validate this is actually a subquery relation
    Assert(rel->relid > 0);
    Assert(planner_rt_fetch(rel->relid, root)->rtekind == RTE_SUBQUERY);

    // Get row count from subquery's cheapest path
    sub_final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
    rel->tuples = sub_final_rel->cheapest_total_path->rows;

    // Estimate column widths from subquery's target list
    foreach(lc, subroot->parse->targetList)
    {
        TargetEntry *te = lfirst_node(TargetEntry, lc);
        Node *texpr = (Node *) te->expr;
        int32 item_width = 0;

        // Skip junk columns and out-of-range columns
        if (te->resjunk)
            continue;
        if (te->resno < rel->min_attr || te->resno > rel->max_attr)
            continue;

        // For simple Vars, use subquery's width estimate
        if (IsA(texpr, Var) && subroot->parse->setOperations == NULL)
        {
            Var *var = (Var *) texpr;
            RelOptInfo *subrel = find_base_rel(subroot, var->varno);
            item_width = subrel->attr_widths[var->varattno - subrel->min_attr];
        }

        rel->attr_widths[te->resno - rel->min_attr] = item_width;
    }

    // Complete size estimation using standard base relation logic
    set_baserel_size_estimates(root, rel);
}
```