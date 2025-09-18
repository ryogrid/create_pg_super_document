# check_agglevels_and_constraints

## Location
src/backend/parser/parse_agg.c: 299 - 635

## Overview
Validates that aggregate functions and grouping operations are used in appropriate SQL contexts and determines their proper nesting levels within query structures.

## Definition
static void check_agglevels_and_constraints(ParseState *pstate, Node *expr)

## Detailed Description
This function serves as the central validation point for aggregate functions and grouping operations in PostgreSQL. It performs two main tasks: first, it determines the minimum variable level for the aggregate by analyzing its arguments, and second, it validates that the aggregate or grouping operation is being used in a legal SQL context.

The function handles both Aggref nodes (regular aggregates) and GroupingFunc nodes (GROUPING() expressions) uniformly, since they have similar nesting and placement restrictions. It calls check_agg_arguments to analyze the aggregate's arguments and determine at which query nesting level the aggregate should be evaluated. Then it marks the appropriate parse state level as containing aggregates.

The function contains an extensive switch statement that validates the context in which the aggregate appears, checking against numerous expression kinds to ensure SQL standard compliance and PostgreSQL-specific rules. It provides detailed error messages for invalid placements like aggregates in WHERE clauses, JOIN conditions, or various constraint expressions.

## Parameters / Member Variables
- `pstate`: Current parse state containing context information and expression kind
- `expr`: Node representing either an Aggref or GroupingFunc to be validated

## Dependencies
- Functions called/Symbols referenced:
  - check_agg_arguments
  - ParseExprKindName
  - ereport (for error handling)
- Called from (representative examples):
  - transformAggregateCall
  - transformGroupingFunc
  - check_ungrouped_columns_context

## Notes and Other Information
- The function treats both aggregate functions and GROUPING operations identically for validation purposes
- Contains comprehensive coverage of all SQL expression contexts where aggregates might appear
- Uses two error reporting schemes: custom messages for complex contexts and generic messages with ParseExprKindName for simple keyword contexts
- The switch statement intentionally has no default case to ensure compiler warnings when new expression kinds are added
- Properly handles query nesting by walking up the parse state hierarchy to mark the correct level as having aggregates