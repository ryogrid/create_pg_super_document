# check_agg_arguments

## Location
src/backend/parser/parse_agg.c: 636 - 716

## Overview
Analyzes aggregate function arguments to determine the semantic nesting level of the aggregate and validates that nested aggregates and variables are used correctly.

## Definition
static int check_agg_arguments(ParseState *pstate, List *directargs, List *args, Expr *filter)

## Detailed Description
This function determines the semantic level at which an aggregate function should be evaluated by examining its arguments and filter expressions. The semantic level is determined by the lowest-level variable or aggregate found in the aggregated arguments (including ORDER BY columns) and filter expressions. A level of 0 represents the current SELECT's level, 1 represents its parent level, and so on.

The function performs several important validations: it detects nested aggregates at the same semantic level (which is illegal), ensures that direct arguments don't contain lower-level variables than the aggregate itself, and prohibits any aggregates in direct arguments at the same or lower level. Direct arguments are treated specially because they are evaluated per-group rather than per-row, following SQL standard semantics.

The function uses a walker pattern to traverse the expression tree, collecting information about the minimum variable and aggregate levels found. It then applies business rules to determine the final aggregate level and validates various nesting constraints.

## Parameters / Member Variables
- `pstate`: Current parse state for error reporting and context
- `directargs`: List of direct arguments to the aggregate (not counted for level determination)
- `args`: List of regular aggregated arguments (used for level determination)
- `filter`: Optional filter expression (FILTER clause, used for level determination)

## Dependencies
- Functions called/Symbols referenced:
  - check_agg_arguments_walker
  - locate_agg_of_level
  - locate_var_of_level
  - check_agg_arguments_context
- Called from (representative examples):
  - check_agglevels_and_constraints
  - check_ungrouped_columns_context

## Notes and Other Information
- Returns the determined semantic level (0 = current level, 1 = parent level, etc.)
- Direct arguments are excluded from level determination per SQL standard requirements
- Nested aggregates at the same semantic level trigger an error to prevent execution ordering issues
- The function validates that outer-level aggregates cannot contain lower-level variables in direct arguments
- Uses a context structure to maintain state during tree walking operations