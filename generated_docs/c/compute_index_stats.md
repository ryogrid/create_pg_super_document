# compute_index_stats

## Location
src/backend/commands/analyze.c: 828 - 998

## Overview
Computes statistics for index expressions and partial index predicates by evaluating them against sampled table rows.

## Definition


## Detailed Description
This function processes index expressions and partial index predicates to generate statistics for the query planner. For each index, it sets up an execution environment to evaluate expressions and predicates against the sampled rows. It handles partial indexes by checking predicate conditions to determine which rows would actually be included in the index. For expression indexes, it evaluates the expressions to extract values for statistical analysis.

The function creates a separate memory context for index processing, sets up executor state and expression contexts for evaluation, and processes each sampled row to evaluate predicates and expressions. It calculates the fraction of rows that satisfy partial index predicates and uses this to estimate the total index size. Finally, it computes statistics for expression columns using the extracted values.

## Parameters / Member Variables
- : The table relation being analyzed
- : Total estimated number of rows in the table
- : Array of AnlIndexData structures containing index information and statistics
- : Number of indexes to process
- : Array of sampled HeapTuple rows from the table
- : Number of rows in the sample
- : Memory context for temporary column statistics computation

## Dependencies
- Functions called/Symbols referenced:
  - /: Executor state management for expression evaluation
  - : Gets per-tuple expression context for evaluation
  - : Prepares predicate for execution
  - : Evaluates index expressions to produce datum values
  - : Evaluates partial index predicates
  - : Copies datum values with proper memory management
  - : Index-specific fetch function for statistics computation
- Called from (representative examples):
  - : Main analysis function when processing indexes

## Notes and Other Information
- Creates a dedicated 'Analyze Index' memory context for index processing
- Skips indexes with no analyzable columns and no partial predicate
- Uses executor state and expression contexts to safely evaluate complex expressions
- Handles both partial indexes (with predicates) and expression indexes
- Calculates  to estimate what fraction of table rows appear in each index
- Processes expression values in strided format for efficient statistics computation
- Properly manages memory contexts to prevent leaks during expression evaluation
- Resets expression context for each row to reclaim temporary memory used during evaluation