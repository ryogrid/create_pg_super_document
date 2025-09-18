# set_baserel_size_estimates

## Location
[src/backend/optimizer/path/costsize.c:5242-5271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5242-L5271)

## Overview
Sets the size estimates for a base relation including output rows, tuple width, and cost of evaluating base restriction clauses.

## Definition


## Detailed Description
This function calculates and sets key size estimation fields for a base relation after its targetlist and restrictinfo list have been constructed. It is a core function in PostgreSQL's query planning process that determines:

1. **Row Count Estimation**: Applies restriction clause selectivity to the base tuple count to estimate filtered output rows
2. **Cost Estimation**: Calculates the cost of evaluating base restriction clauses  
3. **Width Estimation**: Determines the average output tuple width

The function uses  with JOIN_INNER semantics to compute how many tuples will pass the base restriction conditions. The result is clamped to reasonable bounds and stored in .

Cost evaluation for restriction clauses is performed using , storing the result in . Finally,  computes the expected tuple width based on the relation's targetlist.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and configuration
- : RelOptInfo for the base relation being sized (must have relid > 0)

## Dependencies
- Functions called/Symbols referenced:
  - [clauselist_selectivity](../c/clauselist_selectivity.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - [cost_qual_eval](../c/cost_qual_eval.md)
  - [set_rel_width](set_rel_width.md)
  - JOIN_INNER
- Called from (representative examples):
  - [set_plain_rel_size](set_plain_rel_size.md)
  - [set_tablesample_rel_size](set_tablesample_rel_size.md)
  - [set_subquery_size_estimates](set_subquery_size_estimates.md)
  - [set_function_size_estimates](set_function_size_estimates.md)
  - [set_values_size_estimates](set_values_size_estimates.md)

## Notes and Other Information
- Must only be applied to base relations (Assert checks rel->relid > 0)
- Requires that rel->tuples, targetlist, and baserestrictinfo be populated beforehand
- Sets three critical RelOptInfo fields: rows, baserestrictcost, and width (via set_rel_width)
- Uses JOIN_INNER selectivity semantics even though this is for base relations
- Essential function called by various relation type sizing functions throughout the optimizer
- Located in src/backend/optimizer/path/costsize.c:5242-5271