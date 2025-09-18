# parse_analyze_varparams

## Location
src/backend/parser/analyze.c: 144 - 184

## Overview
Analyzes a raw parse tree and transforms it into a Query node, with support for dynamic parameter type deduction from context where parameter types can be modified or enlarged during analysis.

## Definition


## Detailed Description
This function is a variant of parse_analyze_fixedparams that provides more flexibility in handling parameters. Unlike the fixed parameter version, this function allows parameter types to be deduced from context during the analysis process. The paramTypes array can be modified or even enlarged using repalloc() to accommodate newly discovered parameters.

The function follows a similar workflow to parse_analyze_fixedparams but includes additional parameter validation:
1. Creates a parse state structure
2. Sets up variable parameter handling (allowing type deduction)
3. Performs statement transformation
4. Validates that all parameters have been properly resolved
5. Generates query ID and invokes hooks as needed

This approach is particularly useful for dynamic SQL scenarios where parameter information may not be fully known upfront, allowing PostgreSQL to infer parameter types based on how they are used within the query.

## Parameters / Member Variables
- : The raw parse tree structure produced by the SQL parser
- : The original SQL source text (required as of PostgreSQL 8.4)
- : Pointer to array of parameter type OIDs (can be modified/enlarged)
- : Pointer to number of parameters (can be updated)
- : Query environment containing additional context like WITH clause data

## Dependencies
- Functions called/Symbols referenced:
  - make_parsestate: Creates parse state structure
  - setup_parse_variable_parameters: Sets up dynamic parameter handling
  - transformTopLevelStmt: Performs the main statement transformation
  - check_variable_parameters: Validates parameter resolution
  - IsQueryIdEnabled: Checks if query ID generation is enabled
  - JumbleQuery: Generates query ID for statistics
  - free_parsestate: Cleanup parse state structure
  - pgstat_report_query_id: Reports query ID for statistics collection

- Called from (representative examples):
  - pg_analyze_and_rewrite_varparams: Main analysis entry point for variable parameters

## Notes and Other Information
- This function allows more flexible parameter handling compared to parse_analyze_fixedparams
- Parameter arrays can be dynamically resized using repalloc() during analysis
- The check_variable_parameters() call ensures all parameters were properly resolved
- Useful for scenarios where parameter types need to be inferred from usage context
- Still maintains the same post-analysis hooks and query ID generation as the fixed variant
- The function signature uses double pointers for paramTypes and numParams to allow modification