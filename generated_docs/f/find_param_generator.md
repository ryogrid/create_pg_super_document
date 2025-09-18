# find_param_generator

## Location
[src/backend/utils/adt/ruleutils.c:8276-8372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L8276-L8372)

## Overview
Searches for a subplan or initplan that generates the value for a PARAM_EXEC parameter in PostgreSQL's query execution tree.

## Definition
static SubPlan *find_param_generator(Param *param, deparse_context *context, int *column_p)

## Detailed Description
This function attempts to locate the subplan or initplan that emits the value for a PARAM_EXEC parameter by traversing the query execution plan hierarchy. It searches through the current plan node and its ancestors to find a matching parameter generator. The function follows a systematic search pattern: first checking the innermost plan node's initplans, then examining MULTIEXPR_SUBLINK SubPlans in the plan's targetlist, and finally searching through ancestor nodes. When a match is found, it returns the generating SubPlan and sets the output column number.

## Parameters / Member Variables
- `param`: The Param node for which to find the generator (must be PARAM_EXEC type)
- `context`: Deparse context containing namespace information and plan hierarchy
- `column_p`: Output parameter to store the 0-based output column number of the generating subplan

## Dependencies
- Functions called/Symbols referenced:
  - [find_param_generator_initplan](find_param_generator_initplan.md)
  - foreach_node
  - foreach_int
  - foreach_current_index
  - deparse_namespace
  - SubPlan
  - PARAM_EXEC
  - MULTIEXPR_SUBLINK
- Called from (representative examples):
  - [get_parameter](../g/get_parameter.md)

## Notes and Other Information
- Only processes PARAM_EXEC parameters; returns NULL for other parameter types
- Searches both initplans and MULTIEXPR_SUBLINK subplans in the targetlist
- Performs hierarchical search through ancestor nodes in the plan tree
- Returns NULL if no generator is found
- Sets *column_p to 0 initially to prevent compiler warnings
- Part of PostgreSQL's rule deparsing system for query plan visualization