# cost_qual_eval

## Location
src/backend/optimizer/path/costsize.c: 4643 - 4668

## Overview
Estimates the CPU costs of evaluating a WHERE clause, providing both startup and per-tuple evaluation costs.

## Definition


## Detailed Description
The  function computes cost estimates for evaluating boolean qualification expressions (WHERE clause conditions). It processes a list of qualification expressions and calculates both one-time startup costs and per-tuple evaluation costs. The function serves as the main entry point for qualification cost estimation throughout the PostgreSQL query planner.

The function accepts either an implicitly-ANDed list of boolean expressions or a list of RestrictInfo nodes (preferred for caching benefits). It initializes a cost evaluation context and walks through each qualification expression using  to accumulate the total costs. The implicit ANDing at the top level incurs no additional cost.

This function is fundamental to PostgreSQL's cost-based optimization, as qualification evaluation costs significantly impact plan selection decisions across all scan and join methods.

## Parameters / Member Variables
- : Output parameter receiving the calculated QualCost structure with startup and per_tuple components
- : List of qualification expressions (Node* or RestrictInfo*) to evaluate
- : PlannerInfo context for planning information (can be NULL, resulting in slightly worse estimates)

## Dependencies
- Functions called/Symbols referenced:
  - cost_qual_eval_walker
  - cost_qual_eval_context (struct)
  - QualCost (struct)
- Called from (representative examples):
  - cost_index
  - cost_tidscan
  - final_cost_nestloop
  - final_cost_mergejoin
  - final_cost_hashjoin
  - get_restriction_qual_cost
  - set_baserel_size_estimates

## Notes and Other Information
- Widely used throughout the cost estimation system for all plan node types that evaluate qualifications
- RestrictInfo input is preferred over raw expressions because it enables cost caching
- Root parameter can be NULL in some code paths, which reduces estimation accuracy
- Does not charge cost for implicit ANDing at the top level of qualification lists
- Results are accumulated in a cost_qual_eval_context structure before final assignment
- Critical for selectivity and cost estimation in scan operations, joins, and aggregations