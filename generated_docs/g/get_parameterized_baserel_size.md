# get_parameterized_baserel_size

## Location
src/backend/optimizer/path/costsize.c: 5272 - 5320

## Overview
Estimates the number of rows for a parameterized scan of a base relation by applying both additional join clauses and the relation's own restriction clauses.

## Definition


## Detailed Description
This function calculates the expected row count for a parameterized base relation scan, which occurs when a relation is accessed with additional constraints from outer relations in a join. The function:

1. **Combines all clauses**: Concatenates the parameter clauses (from outer relations) with the relation's own base restriction clauses
2. **Computes selectivity**: Uses  with the relation's relid (not 0) to treat clauses as non-join clauses during selectivity estimation
3. **Applies to base tuples**: Multiplies the combined selectivity by the relation's base tuple count
4. **Ensures safety**: Caps the result to not exceed the base relation's row estimate (rel->rows)

The key difference from  is that this function includes additional parameter clauses that come from outer relations in join contexts, providing a more accurate estimate for parameterized access paths.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state
- : RelOptInfo for the base relation being sized
- : List of additional join clauses to be applied as parameters

## Dependencies
- Functions called/Symbols referenced:
  - list_concat_copy
  - clauselist_selectivity
  - clamp_row_est
  - JOIN_INNER
- Called from (representative examples):
  - get_baserel_parampathinfo

## Notes and Other Information
- Must be called after  has been applied to the relation
- Uses the relation's relid (not 0) in clauselist_selectivity to force treating clauses as non-join clauses
- Provides a safety check ensuring parameterized estimates don't exceed base estimates
- Essential for accurate costing of parameterized index scans and other parameterized access methods
- The result represents row count after applying both parameter constraints and base restrictions
- Located in src/backend/optimizer/path/costsize.c:5272-5320