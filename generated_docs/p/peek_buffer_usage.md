# peek_buffer_usage

## Location
src/backend/commands/explain.c: 3703 - 3742

## Overview
Determines whether buffer usage statistics contain any non-zero values that would be worth displaying in EXPLAIN output.

## Definition
```c
static bool peek_buffer_usage(ExplainState *es, const BufferUsage *usage)
```

## Detailed Description
This function serves as a predicate to determine whether buffer usage statistics should be displayed in EXPLAIN output. It examines all the buffer usage counters and timing information to see if any contain meaningful (non-zero) values. The function handles different output formats differently: for non-text formats (JSON, XML, YAML), it always returns true to include complete statistics, while for text format it only returns true if there are actual non-zero values to avoid cluttering the output.

The function checks three categories of buffer usage: shared buffers, local buffers, and temporary buffers, along with their associated timing information when timing is enabled.

## Parameters / Member Variables
- `es`: ExplainState containing output format information to determine display behavior
- `usage`: BufferUsage structure containing buffer statistics including hit/read/dirty/write counters and timing data

## Dependencies
- Functions called/Symbols referenced:
  - EXPLAIN_FORMAT_TEXT
  - INSTR_TIME_IS_ZERO (macro for checking zero timing values)
  - BufferUsage (structure with buffer counters)
- Called from (representative examples):
  - ExplainOnePlan
  - ExplainPrintSerialize

## Notes and Other Information
- Returns false immediately if usage parameter is NULL
- For structured output formats, always returns true to ensure complete data representation
- For text format, only returns true if at least one buffer counter or timing value is non-zero
- Checks all buffer categories: shared_blks_*, local_blks_*, temp_blks_*, and their associated timing counters
- Used to avoid displaying empty buffer usage sections in EXPLAIN output when no buffer activity occurred
- Part of PostgreSQL's buffer usage tracking and reporting infrastructure for query performance analysis