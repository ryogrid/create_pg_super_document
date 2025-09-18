# set_foreign_size_estimates

## Location
[src/backend/optimizer/path/costsize.c:6067-6101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L6067-L6101)

## Overview
Sets preliminary size estimates for a foreign table base relation, providing default values that will be refined by the foreign-data wrapper's GetForeignRelSize function.

## Definition
```c
void set_foreign_size_estimates(PlannerInfo *root, RelOptInfo *rel)
```

## Detailed Description
This function provides initial size estimates for foreign tables, which are external data sources managed by foreign-data wrappers (FDWs). Since PostgreSQL cannot directly analyze foreign tables to gather accurate statistics, this function sets up reasonable default estimates that serve as a starting point. The function sets a deliberately "bogus" default row estimate of 1000, calculates the cost of evaluating base restrictions using available restrictinfo, and computes relation width based on datatype-driven estimates from the targetlist. The expectation is that the foreign-data wrapper will provide more accurate estimates through its GetForeignRelSize callback function shortly after this initial setup.

The function focuses on estimates that can be reasonably calculated without deep knowledge of the foreign data source: restriction evaluation costs and tuple width based on PostgreSQL data types.

## Parameters / Member Variables
- `root`: Pointer to PlannerInfo structure containing global planning information and context
- `rel`: Pointer to RelOptInfo structure representing the foreign table relation for size estimation

## Dependencies
- Functions called/Symbols referenced:
  - [cost_qual_eval](../c/cost_qual_eval.md)
  - [set_rel_width](set_rel_width.md)
- Called from (representative examples):
  - [set_foreign_size](set_foreign_size.md)

## Notes and Other Information
- Only applicable to base relations (validated with assertions)
- Uses a deliberately "bogus" default row estimate of 1000 as a placeholder
- Relies on foreign-data wrapper's GetForeignRelSize function to provide accurate estimates
- Must be called after the relation's targetlist and restrictinfo list are constructed
- Cannot provide meaningful row count estimates without FDW-specific knowledge
- Part of PostgreSQL's foreign data wrapper infrastructure within the query optimizer