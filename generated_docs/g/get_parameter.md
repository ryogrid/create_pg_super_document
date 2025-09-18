# get_parameter

## Location
src/backend/utils/adt/ruleutils.c: 8394 - 8530

## Overview
Displays a Param node appropriately by locating and formatting its corresponding expression or generating a suitable textual representation.

## Definition
static void get_parameter(Param *param, deparse_context *context)

## Detailed Description
This function is responsible for converting a Param node into its appropriate textual representation during query deparsing. It employs a multi-step approach to resolve parameters: first attempting to find the referent expression for PARAM_EXEC parameters, then checking for subplan generators, handling PARAM_EXTERN parameters with function argument names, and finally falling back to a simple $N format. The function handles various parameter types including execution parameters, external parameters, and manages proper scoping and qualification of parameter names.

## Parameters / Member Variables
- `param`: The Param node to be displayed
- `context`: Deparse context containing buffer, namespaces, and formatting options

## Dependencies
- Functions called/Symbols referenced:
  - [find_param_referent](../f/find_param_referent.md)
  - [find_param_generator](../f/find_param_generator.md)
  - [push_ancestor_plan](../p/push_ancestor_plan.md)
  - [pop_ancestor_plan](../p/pop_ancestor_plan.md)
  - get_rule_expr
  - [quote_identifier](../q/quote_identifier.md)
  - llast
  - deparse_namespace
  - SubPlan
  - PARAM_EXTERN
  - Aggref
  - GroupingFunc
- Called from (representative examples):
  - get_rule_expr

## Notes and Other Information
- Handles three main cases: parameter referents, subplan outputs, and external parameters
- For PARAM_EXEC parameters, tries to locate the original expression from ancestor plan nodes
- For subplan outputs, formats as "(subplan_name).colN" notation
- For external parameters, attempts to use function argument names when available
- Applies proper parentheses around complex expressions to maintain atomicity
- Forces variable prefixing when displaying expressions from different plan nodes
- Qualifies parameter names when multiple namespaces exist to avoid ambiguity
- Falls back to simple $N format when other methods fail
- Contains an assertion that non-external parameters should be resolvable