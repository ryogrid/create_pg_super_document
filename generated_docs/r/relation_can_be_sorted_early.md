# relation_can_be_sorted_early

## Location
src/backend/optimizer/path/equivclass.c: 917 - 1027

## Overview
Determines whether a relation can be sorted on a given EquivalenceClass before the final output step, during the scan/join part of the plan.

## Definition


## Detailed Description
This function evaluates whether a relation can be sorted early on a specific EquivalenceClass during query execution, before reaching the final output stage. The function employs a two-phase approach: first attempting to find an EC member that directly matches a target expression in the relation's target list, and then trying to find an expression that can be computed from the available target expressions.

The function implements several safety checks to ensure the sort can be performed early, including rejecting volatile expressions, set-returning functions, and optionally non-parallel-safe expressions. Early sorting is beneficial for query optimization as it can enable more efficient join algorithms and reduce overall execution time.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state information
- : RelOptInfo structure representing the relation to be potentially sorted
- : EquivalenceClass to test for early sortability
- : If true, non-parallel-safe expressions are rejected

## Dependencies
- Functions called/Symbols referenced:
  - find_ec_member_matching_expr
  - expression_returns_set
  - is_parallel_safe
  - find_computable_ec_member
- Called from (representative examples):
  - get_useful_pathkeys_for_relation

## Notes and Other Information
- Volatile EquivalenceClasses are immediately rejected as such sorts must be postponed
- Set-returning functions are rejected because they cannot be computed early in the plan
- The function first tries to find direct matches in the relation's target list before attempting computed expressions
- Parallel safety checking is performed last as it's an expensive operation
- Returns true if any suitable EC member is found, false otherwise
- Located in src/backend/optimizer/path/equivclass.c:917-1027