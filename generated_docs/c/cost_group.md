# cost_group

## Location
src/backend/optimizer/path/costsize.c: 3163 - 3232

## Overview
Determines and returns the cost of performing a Group plan node, including the cost of its input data which must be appropriately sorted.

## Definition


## Detailed Description
This function calculates the execution cost for a Group operation in PostgreSQL's query planner. The Group operation is used to eliminate duplicate rows and implement grouping functionality. The cost calculation includes:

1. **Grouping comparison costs**: Charges cpu_operator_cost per comparison per input tuple, assuming all grouping columns are compared for most tuples during the grouping process.

2. **HAVING clause evaluation**: If HAVING qualifications are present, accounts for their evaluation cost and applies their selectivity to reduce the estimated output tuple count.

3. **Output tuple estimation**: The number of output tuples is initially set to numGroups but may be reduced by HAVING clause selectivity.

The function assumes the input data is already sorted appropriately for the grouping operation, which is a prerequisite for the Group plan node to function correctly.

## Parameters / Member Variables
- : The Path structure to be updated with calculated costs and row estimates
- : PlannerInfo structure containing planner context and statistics
- : Number of columns used for grouping comparisons
- : Estimated number of distinct groups in the result
- : List of HAVING qualification clauses (can be NULL)
- : Startup cost of the input data source
- : Total cost of the input data source  
- : Estimated number of input tuples

## Dependencies
- Functions called/Symbols referenced:
  - [cost_qual_eval](cost_qual_eval.md)
  - [clamp_row_est](clamp_row_est.md)
  - [clauselist_selectivity](clauselist_selectivity.md)
  - QualCost
  - Cost
  - JOIN_INNER
- Called from (representative examples):
  - [create_group_path](create_group_path.md)
  - [choose_hashed_setop](choose_hashed_setop.md)

## Notes and Other Information
- Caller must ensure input costs are for appropriately-sorted input data
- The cost model assumes all grouping columns are compared for most input tuples
- HAVING clause selectivity is applied to refine output tuple estimates
- The function uses clamp_row_est to ensure row estimates remain within reasonable bounds
- Group operations require pre-sorted input to function efficiently