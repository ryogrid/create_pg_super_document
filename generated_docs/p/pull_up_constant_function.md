# pull_up_constant_function

## Location
src/backend/optimizer/prep/prepjointree.c: 1954 - 2071

## Overview
This function pulls up an RTE_FUNCTION expression that was simplified to a constant, hoisting the constant value up into the parent query and replacing the RTE_FUNCTION with RTE_RESULT.

## Definition


## Detailed Description
The pull_up_constant_function optimization is part of PostgreSQL's query preprocessing phase that simplifies function calls that have been reduced to constants. When a FUNCTION RTE contains only a Const expression, this function extracts that constant value and propagates it throughout the parent query, eliminating the need to scan the function RTE.

The function performs several safety checks before proceeding:
- Ensures the RTE doesn't have ORDINALITY (not implemented)  
- Verifies there's exactly one function in the RTE
- Confirms the function expression is a Const
- Checks that the result is scalar (single column, not composite)

After validation, it creates a pullup_replace_vars_context to systematically replace all references to the RTE's output with copies of the constant expression. The original RTE is then converted to RTE_RESULT type, indicating it no longer needs to be scanned.

The optimization is conservative - it only handles simple Const expressions rather than any immutable expression to avoid multiple evaluations and ensure the constant can participate in further constant folding.

## Parameters / Member Variables
- : PlannerInfo structure containing the query being optimized
- : RangeTblRef node that has been identified as a FUNCTION RTE  
- : The RangeTblEntry being processed for constant pullup
- : AppendRelInfo if this RTE is part of an appendrel, used to determine if PlaceHolderVar wrapping is needed

## Dependencies
- Functions called/Symbols referenced:
  - linitial_node
  - get_expr_result_type  
  - makeTargetEntry
  - perform_pullup_replace_vars
  - TYPEFUNC_SCALAR
  - RTE_RESULT
- Called from:
  - pull_up_subqueries_recurse

## Notes and Other Information
- The pulled-up constant may need to be wrapped in a PlaceHolderVar if the RTE is below an outer join or part of an appendrel
- PlaceHolderVar wrapping is also required when the parent query uses grouping sets
- The function is conservative in only handling Const expressions to avoid performance issues from multiple evaluations
- The main benefit is enabling further constant folding optimizations in the parent query
- Located in src/backend/optimizer/prep/prepjointree.c:1954-2071