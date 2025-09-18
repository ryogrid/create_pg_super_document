# get_agg_clause_costs

## Location
src/backend/optimizer/prep/prepagg.c: 560 - 696

## Overview
Processes the PlannerInfo's aggregate transition and aggregate info lists to accumulate cost information for aggregate clauses, taking into account the expected partial-aggregation mode.

## Definition


## Detailed Description
This function is a critical component of PostgreSQL's query optimizer that calculates the cost estimates for aggregate operations. It processes two key lists from PlannerInfo: 'aggtransinfos' and 'agginfos', accumulating cost information based on the expected partial-aggregation mode specified by the aggsplit parameter.

The function performs several key operations:

1. **Transition Function Costs**: For each AggTransInfo, it adds the cost of aggregate transitions using either the transition function (transfn) or combine function (combinefn) depending on the aggregation split mode.

2. **Serialization/Deserialization Costs**: Accounts for costs of serializing and deserializing transition states when data needs to be passed between different aggregation phases.

3. **Input Expression and Filter Costs**: Calculates costs for evaluating input expressions and any aggregate filters, but only for initial aggregate nodes to avoid double-counting.

4. **Memory Space Estimation**: Estimates the total space needed for transition states, considering different data types and their storage requirements (pass-by-value vs pass-by-reference).

5. **Final Function Costs**: For each AggInfo, adds the cost of running the final function and processing direct arguments.

The costs are ADDED to the existing values in the costs structure, so the caller must initialize it to zero beforehand.

## Parameters / Member Variables
- : PlannerInfo structure containing the aggregate information lists and planner context
- : AggSplit enum value indicating the expected partial-aggregation mode, which affects cost estimates
- : AggClauseCosts structure where the calculated costs will be accumulated

## Dependencies
- Functions called/Symbols referenced:
  - add_function_cost
  - cost_qual_eval_node
  - get_typavgwidth
  - linitial_node
  - DO_AGGSPLIT_COMBINE
  - DO_AGGSPLIT_DESERIALIZE
  - DO_AGGSPLIT_SERIALIZE
  - DO_AGGSPLIT_SKIPFINAL
  - ALLOCSET_SMALL_INITSIZE
  - ALLOCSET_DEFAULT_INITSIZE
- Called from (representative examples):
  - create_grouping_paths (src/backend/optimizer/plan/planner.c:3832)
  - create_partial_grouping_paths (src/backend/optimizer/plan/planner.c:7363, 7367)

## Notes and Other Information
- The function handles special cases for different transition types, including INTERNAL types and array aggregates
- Memory space estimation is particularly sophisticated, accounting for different storage patterns like expanded arrays used by array_append()
- The function respects the aggregation split mode to avoid double-counting costs in multi-phase aggregation scenarios
- Cost calculations distinguish between startup costs (one-time) and per-tuple costs (repeated for each input row)
- The function is essential for the optimizer to make informed decisions about aggregation strategies (HashAgg vs GroupAgg, partial vs full aggregation)