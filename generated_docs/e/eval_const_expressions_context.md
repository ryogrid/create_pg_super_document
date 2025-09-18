# eval_const_expressions_context

## Location
src/backend/optimizer/util/clauses.c: 66 - 72

## Overview
A context structure used during constant expression evaluation and simplification in the PostgreSQL query optimizer to track state and parameters needed for the recursive expression mutation process.

## Definition


## Detailed Description
The eval_const_expressions_context structure serves as a state container for the constant expression evaluation process in PostgreSQL's query optimizer. This context is passed through the recursive expression tree walking functions to maintain necessary information for safe and correct constant folding operations. The structure supports two primary modes of operation: safe transformations only (when estimate=false) and unsafe transformations allowed (when estimate=true), enabling different levels of optimization aggressiveness depending on the usage context.

## Parameters / Member Variables
- : ParamListInfo containing bound parameter values for parameter substitution during evaluation
- : PlannerInfo pointer for tracking inlined-function dependencies and accessing planner state
- : List of functions currently being recursively simplified to prevent infinite recursion
- : Node pointer to the current CASE expression value being examined for CASE optimization
- : Boolean flag indicating whether unsafe transformations are allowed (true for estimation, false for safe-only)

## Dependencies
- Functions called/Symbols referenced:
  - [ParamListInfo](../P/ParamListInfo.md) (parameter information structure)
  - [PlannerInfo](../P/PlannerInfo.md) (planner state structure)
  - [List](../L/List.md) (PostgreSQL list structure)
  - [Node](../N/Node.md) (expression tree node)
- Called from (representative examples):
  - [eval_const_expressions](eval_const_expressions.md)
  - [estimate_expression_value](estimate_expression_value.md)
  - [eval_const_expressions_mutator](eval_const_expressions_mutator.md)

## Notes and Other Information
This context structure is fundamental to PostgreSQL's constant expression folding optimization. The estimate flag distinguishes between two usage patterns: conservative constant folding during actual query planning (estimate=false) where only immutable functions are folded, and aggressive estimation (estimate=true) where stable functions may also be folded for cost estimation purposes. The active_fns list prevents infinite recursion when simplifying recursive function calls, while case_val enables specialized CASE expression optimizations.