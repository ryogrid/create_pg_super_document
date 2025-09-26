# get_agg_expr

## Location
src/backend/utils/adt/ruleutils.c: 10561 - 10572

## Overview
A simple wrapper function that parses back an Aggref (aggregate reference) node into its human-readable string representation by delegating to the more comprehensive get_agg_expr_helper function.

## Definition


## Detailed Description
This static function serves as a convenient entry point for deparsing aggregate expressions in PostgreSQL's rule deparsing system. It provides a simplified interface to the more complex get_agg_expr_helper function by supplying default values for optional parameters.

The function acts as a thin wrapper that calls get_agg_expr_helper with NULL values for the proname and pronamespace parameters and false for the use_variadic flag, which represents the most common case for aggregate expression deparsing.

## Parameters / Member Variables
- : Pointer to the Aggref node containing the aggregate expression to be deparsed
- : Deparse context containing the output buffer and formatting preferences
- : Pointer to the original Aggref node, used for comparison and context in complex aggregate expressions

## Dependencies
- Functions called/Symbols referenced:
  - get_agg_expr_helper (the main implementation function for aggregate expression deparsing)
  - Aggref (aggregate reference node structure)
  - deparse_context (deparsing context structure)
- Called from:
  - get_rule_expr (main expression deparsing dispatcher)
  - get_agg_combine_expr (for combining aggregate expressions)

## Notes and Other Information
- This function is part of PostgreSQL's rule deparsing system used for displaying views, rules, and constraints
- The wrapper design allows for a clean separation between the simple common case and more complex aggregate expression handling
- The original_aggref parameter enables context-aware deparsing when dealing with nested or modified aggregate expressions
- All the actual formatting work is delegated to get_agg_expr_helper with sensible defaults