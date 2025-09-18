# parse_analyze_fixedparams

## Location
src/backend/parser/analyze.c: 104 - 143

## Overview
Analyzes a raw parse tree and transforms it into a Query node, with support for pre-defined parameter types where references to undefined parameter indexes are disallowed.

## Definition


## Detailed Description
This function serves as one of the main entry points for SQL parsing and analysis in PostgreSQL. It takes a raw parse tree (produced by the parser) and transforms it into a fully analyzed Query node that can be used by the planner and executor. The function specifically handles cases where parameter types are known in advance, making it suitable for prepared statements and similar scenarios where parameter information is available upfront.

The function performs several key operations:
1. Creates a parse state structure to track parsing context
2. Sets up parameter type information if provided
3. Transforms the raw statement using the top-level transformation logic
4. Generates a query ID for statistics if enabled
5. Invokes post-parse analysis hooks if configured
6. Reports query statistics

## Parameters / Member Variables
- : The raw parse tree structure produced by the SQL parser
- : The original SQL source text (required as of PostgreSQL 8.4)
- : Array of parameter type OIDs for  parameter references
- : Number of parameters in the paramTypes array
- : Query environment containing additional context like WITH clause data

## Dependencies
- Functions called/Symbols referenced:
  - make_parsestate: Creates parse state structure
  - setup_parse_fixed_parameters: Sets up parameter type information
  - transformTopLevelStmt: Performs the main statement transformation
  - IsQueryIdEnabled: Checks if query ID generation is enabled
  - JumbleQuery: Generates query ID for statistics
  - free_parsestate: Cleanup parse state structure
  - pgstat_report_query_id: Reports query ID for statistics collection

- Called from (representative examples):
  - DefineView: Used when creating views
  - pg_analyze_and_rewrite_fixedparams: Main analysis entry point in postgres.c

## Notes and Other Information
- This function is part of the parse analysis phase, distinct from the raw parsing phase
- The function ensures that only pre-defined parameters (via paramTypes) can be referenced
- Post-parse analysis hooks allow extensions to modify or inspect the analyzed query
- Query ID generation supports query statistics and monitoring features
- The function handles both optimizable SQL statements and utility statements differently