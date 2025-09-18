# substitute_actual_srf_parameters_context

## Location
src/backend/optimizer/util/clauses.c: 80 - 85

## Overview
A context structure used during parameter substitution in Set-Returning Function (SRF) queries to track arguments and sublevel adjustments when replacing Param nodes within subqueries.

## Definition


## Detailed Description
The substitute_actual_srf_parameters_context structure provides specialized context for parameter substitution in Set-Returning Function scenarios, particularly when dealing with subqueries. Unlike the standard parameter substitution context, this structure includes sublevel tracking to properly handle variable scoping when parameters are substituted into nested query structures. The context ensures that parameter substitution correctly adjusts variable reference levels as the mutation process traverses through different query nesting levels.

## Parameters / Member Variables
- : Integer specifying the total number of arguments available for substitution
- : List containing the actual parameter values/expressions to substitute for Param nodes
- : Integer tracking the current nesting level relative to the original query context for proper variable level adjustment

## Dependencies
- Functions called/Symbols referenced:
  - List (PostgreSQL list structure)
  - int (integer type)
  - Query (query tree structure)
  - IncrementVarSublevelsUp (function for adjusting variable levels)
- Called from (representative examples):
  - substitute_actual_srf_parameters
  - substitute_actual_srf_parameters_mutator

## Notes and Other Information
This context structure is specifically designed for Set-Returning Function parameter substitution, which requires special handling of variable scoping across query levels. The key difference from standard parameter substitution is the sublevels_up tracking and the use of IncrementVarSublevelsUp to adjust variable reference levels when inserting parameters into subqueries. The substitution process only handles PARAM_EXTERN parameters and performs deep copying of parameter values with proper level adjustments to maintain correct variable scoping semantics. The context starts with sublevels_up=1 and increments/decrements as it traverses nested Query nodes.