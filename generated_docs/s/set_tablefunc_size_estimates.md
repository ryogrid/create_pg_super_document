# set_tablefunc_size_estimates

## Location
[src/backend/optimizer/path/costsize.c:5913-5934](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5913-L5934)

## Overview
Sets the size estimates for a base relation that represents a table function, using a fixed estimate of 100 rows for table function calls.

## Definition

```c
void
set_tablefunc_size_estimates(PlannerInfo *root, RelOptInfo *rel)
```
## Detailed Description
This function provides size estimation for relations that represent table functions (RTE_TABLEFUNC) in PostgreSQL queries. Unlike regular function calls that may have varying return set sizes, table functions receive a simple fixed estimate of 100 tuples. This is a conservative heuristic used when more precise cardinality estimation is not available or practical for table function constructs.

The function performs basic validation to ensure it's operating on a valid base relation with a table function range table entry type, then sets a fixed tuple count and delegates remaining size estimation work to .

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and global information
- : RelOptInfo structure representing the relation being sized, must be a base relation with table function RTE

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - RTE_TABLEFUNC
  - [set_baserel_size_estimates](set_baserel_size_estimates.md)
- Called from (representative examples):
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- Uses a hardcoded estimate of 100 rows for all table functions
- Should only be applied to base relations with RTE_TABLEFUNC range table entry type
- The fixed estimate approach reflects the difficulty in accurately predicting table function cardinality
- Part of PostgreSQL's cost-based optimizer for handling special relation types
- Uses assertions to verify proper relation type before processing

## Simplified Source

```c
void
set_tablefunc_size_estimates(PlannerInfo *root, RelOptInfo *rel)
{
    // Verify this is a base relation representing a table function
    Assert(rel->relid > 0);
    Assert(planner_rt_fetch(rel->relid, root)->rtekind == RTE_TABLEFUNC);

    // Use fixed estimate of 100 rows for table functions
    rel->tuples = 100;

    // Complete the size estimation using standard base relation logic
    set_baserel_size_estimates(root, rel);
}
```