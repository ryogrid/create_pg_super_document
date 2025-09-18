# find_param_generator_initplan

## Location
src/backend/utils/adt/ruleutils.c: 8373 - 8393

## Overview
A helper function that searches through a single Plan node's initplans to find a subplan that generates a specific PARAM_EXEC parameter.

## Definition
static SubPlan *find_param_generator_initplan(Param *param, Plan *plan, int *column_p)

## Detailed Description
This function serves as a subroutine for find_param_generator, focusing on searching within a specific Plan node's initialization plans (initplans). It iterates through all SubPlans in the given plan's initPlan list and examines each subplan's setParam list to find a match with the target parameter ID. When a matching parameter is found, it returns the corresponding SubPlan and sets the output column index.

## Parameters / Member Variables
- `param`: The Param node containing the parameter ID to search for
- `plan`: The Plan node whose initplans should be searched
- `column_p`: Output parameter to store the 0-based column index where the parameter is found in the subplan's setParam list

## Dependencies
- Functions called/Symbols referenced:
  - foreach_node
  - foreach_int
  - foreach_current_index
  - SubPlan
  - Param
- Called from (representative examples):
  - [find_param_generator](find_param_generator.md)

## Notes and Other Information
- This is a focused search function that only examines initplans within a single Plan node
- Returns NULL if no matching parameter is found in any of the initplans
- Uses PostgreSQL's foreach_node and foreach_int macros for efficient list iteration
- Sets the column position using foreach_current_index when a match is found
- Part of the parameter resolution mechanism in PostgreSQL's query deparsing system