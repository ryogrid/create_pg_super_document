# get_parameterized_baserel_size

## Location
[src/backend/optimizer/path/costsize.c:5272-5320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5272-L5320)

## Overview
Estimates the number of rows for a parameterized scan of a base relation by applying both additional join clauses and the relation's own restriction clauses.

## Definition

```c
structed already, and a
 * restriction clause list that matches the given component rels must
 * be provided.
 *
 * Since there is more than one way to make a joinrel for more than two
 * base relations, the results we get here could depend on which component
 * rel pair is provided.  In theory we should get the same answers no matter
 * which pair is provided;
```
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
  - [list_concat_copy](../l/list_concat_copy.md)
  - [clauselist_selectivity](../c/clauselist_selectivity.md)
  - [clamp_row_est](../c/clamp_row_est.md)
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