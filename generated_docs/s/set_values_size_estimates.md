# set_values_size_estimates

## Location
[src/backend/optimizer/path/costsize.c:5935-5966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5935-L5966)

## Overview
Sets the size estimates for a base relation that represents a VALUES list, providing precise cardinality based on the actual number of value rows specified.

## Definition

```c
void
set_values_size_estimates(PlannerInfo *root, RelOptInfo *rel)
```
## Detailed Description
This function calculates size estimates for relations that represent VALUES clauses in SQL queries (e.g., VALUES (1,2), (3,4), (5,6)). Unlike other estimation functions that use heuristics or statistical approximations, this function can provide an exact row count since VALUES lists have a known, fixed number of rows determined at parse time.

The function retrieves the range table entry for the VALUES relation, counts the number of value lists specified in the VALUES clause using , and sets this as the precise tuple count for the relation. This accuracy makes VALUES clause estimation one of the most reliable cardinality estimates in PostgreSQL's optimizer.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and global information
- : RelOptInfo structure representing the relation being sized, must be a base relation with VALUES RTE

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - RTE_VALUES
  - list_length (implicitly via rte->values_lists)
  - [set_baserel_size_estimates](set_baserel_size_estimates.md)
- Called from (representative examples):
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- Provides exact cardinality estimation rather than approximation since VALUES list length is known
- Should only be applied to base relations with RTE_VALUES range table entry type
- Does not account for potential set-returning functions within VALUES items (noted as acceptable limitation)
- The precise counting makes this one of the most accurate size estimation functions in the optimizer
- Uses assertions to verify proper relation type and valid relid before processing