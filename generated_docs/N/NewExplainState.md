# NewExplainState

## Location
src/backend/commands/explain.c: 372 - 388

## Overview
NewExplainState creates and initializes a new ExplainState structure with default options for EXPLAIN command processing.

## Definition


## Detailed Description
NewExplainState is a constructor function that allocates and initializes an ExplainState structure. It uses palloc0 to zero-initialize all fields, then sets the default values for EXPLAIN options. By default, only the costs option is enabled (set to true), while other options like analyze, verbose, buffers, etc., remain false. The function also initializes the output string buffer using makeStringInfo(), which creates a dynamically expandable StringInfo structure for accumulating the explain output.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ExplainState (struct type)
  - palloc0
  - makeStringInfo
- Called from (representative examples):
  - ExplainQuery

## Notes and Other Information
- Uses palloc0 for zero-initialization, ensuring all boolean flags start as false
- Only the 'costs' option is enabled by default, reflecting PostgreSQL's standard EXPLAIN behavior
- The StringInfo buffer is pre-allocated to handle output accumulation efficiently
- Memory is allocated in the current memory context (typically the query execution context)