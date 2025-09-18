# initial_cost_mergejoin

## Location
src/backend/optimizer/path/costsize.c: 3514 - 3744

## Overview
Provides a preliminary estimate of the cost of a mergejoin path, producing lower-bound estimates to quickly evaluate path viability before detailed costing.

## Definition


## Detailed Description
This function performs the first phase of merge join cost estimation in PostgreSQL's query planner. It quickly produces lower-bound estimates by:

1. **Scan selectivity analysis**: Uses cached selectivity estimates from the first merge clause to determine what fraction of each input will actually be scanned. Merge joins can terminate early when one input is exhausted (except for full outer joins).

2. **Sort cost calculation**: If either input requires sorting (indicated by non-NULL sortkeys), calculates sorting costs using cost_sort. The function accounts for partial sorting costs based on selectivity estimates.

3. **Input processing estimation**: Estimates startup costs including sort setup and the portion of input that must be read before the first join pair is found. Run costs account for the remaining input that will be processed.

4. **Join type handling**: Adjusts selectivity estimates for different join types:
   - LEFT/ANTI joins: Force outer relation to be fully scanned
   - RIGHT/RIGHT_ANTI joins: Force inner relation to be fully scanned
   - FULL joins: Both relations must be fully processed

5. **Deferred analysis**: Excludes CPU costs and detailed qualification evaluation to maintain speed, leaving these for final_cost_mergejoin.

The function protects against zero row counts and uses clamp_row_est to ensure reasonable estimates.

## Parameters / Member Variables
- : PlannerInfo structure containing planner context and statistics
- : JoinCostWorkspace structure to be filled with preliminary cost estimates and intermediate data
- : Type of join operation (INNER, LEFT, RIGHT, FULL, SEMI, ANTI, etc.)
- : List of join clauses to be used as merge clauses
- : Path representing the outer input to the join
- : Path representing the inner input to the join
- : List of sort keys for outer path (NULL if already sorted)
- : List of sort keys for inner path (NULL if already sorted)
- : JoinPathExtraData containing miscellaneous join information

## Dependencies
- Functions called/Symbols referenced:
  - cost_sort
  - cached_scansel
  - clamp_row_est
  - bms_is_subset
  - JoinCostWorkspace
  - JoinType
  - JoinPathExtraData
  - PathKey
  - MergeScanSelCache
  - Cost
  - JOIN_FULL, JOIN_LEFT, JOIN_ANTI, JOIN_RIGHT, JOIN_RIGHT_ANTI
- Called from (representative examples):
  - try_mergejoin_path
  - try_partial_mergejoin_path

## Notes and Other Information
- This is the first phase of a two-phase merge join costing process
- Uses cached selectivity results from mergejoinscansel() to avoid expensive recomputation
- Sort keys should be NIL when the respective source path is already properly ordered
- CPU costs and detailed join qualification analysis are deferred to final_cost_mergejoin
- Selectivity estimates are readjusted after rounding to maintain accuracy with small input sizes
- The function assumes cost_sort is efficient enough for use in preliminary estimation
- Inner input cost considerations (rescanning, materialization) are partially deferred
- Workspace structure preserves intermediate data for final costing phase