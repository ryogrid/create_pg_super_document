# prepareCommandsInPipeline

## Location
src/bin/pgbench/pgbench.c: 3122 - 3154

## Overview
The prepareCommandsInPipeline function prepares all SQL commands within a pipeline block (between \startpipeline and \endpipeline) for efficient batch execution.

## Definition
```c
static void prepareCommandsInPipeline(CState *st)
```

## Detailed Description
This function handles the preparation of all SQL commands contained within a pipeline block in pgbench scripts. Pipeline mode allows sending multiple queries to the server without waiting for individual results, improving performance for batch operations.

The function operates by:
1. Verifying that the current command is a \startpipeline meta-command
2. Lazily allocating the prepared statement tracking array if needed
3. Checking if this pipeline has already been processed (using the \startpipeline command's prepared flag)
4. Iterating through commands until finding the matching \endpipeline
5. Calling prepareCommand for each SQL command within the pipeline
6. Marking the \startpipeline as processed to avoid redundant preparation

The function uses the \startpipeline command's prepared flag as a marker to track whether the entire pipeline has been processed, even though the \startpipeline itself is not actually prepared.

## Parameters / Member Variables
- `st`: Pointer to CState structure representing the client connection state, containing the current command position and script information

## Dependencies
- Functions called/Symbols referenced:
  - allocCStatePrepared (for lazy allocation of tracking array)
  - prepareCommand (to prepare individual SQL commands)
  - Command (command structure)
  - META_COMMAND, META_STARTPIPELINE, META_ENDPIPELINE (meta-command types)
- Called from (representative examples):
  - executeMetaCommand

## Notes and Other Information
- This function is essential for pgbench's pipeline mode functionality introduced in PostgreSQL 14
- Pipeline mode can significantly improve performance for workloads with many small queries
- The function includes an assertion to verify it's called on a \startpipeline command
- The prepared flag on \startpipeline serves as an optimization to avoid re-preparing pipeline commands
- Only SQL commands within the pipeline are prepared; meta-commands are skipped
- The function does not advance the command counter, leaving that to the caller