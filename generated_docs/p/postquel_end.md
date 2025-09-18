# postquel_end

## Location
src/backend/executor/functions.c: 911 - 930

## Overview
Shuts down execution of a single execution_state node, performing cleanup operations and freeing resources associated with query execution.

## Definition


## Detailed Description
postquel_end is responsible for the orderly shutdown of query execution for a SQL function's execution state. It performs several critical cleanup operations: marks the execution status as done to prevent duplicate shutdown operations, conditionally calls ExecutorFinish and ExecutorEnd for non-utility commands, destroys the destination receiver, and frees the QueryDesc structure. This function ensures that all resources allocated during query execution are properly released and that the execution state is left in a clean, completed state.

## Parameters / Member Variables
- : A pointer to the execution_state structure that needs to be shut down

## Dependencies
- Functions called/Symbols referenced:
  - ExecutorFinish
  - ExecutorEnd
  - FreeQueryDesc
- Called from (representative examples):
  - fmgr_sql
  - ShutdownSQLFunction

## Notes and Other Information
- The function first sets the status to F_EXEC_DONE to prevent duplicate ExecutorEnd calls
- Utility commands (CMD_UTILITY) bypass the Executor framework and don't require ExecutorFinish/ExecutorEnd
- The destination receiver's rDestroy method is called to clean up output handling
- After cleanup, the QueryDesc pointer is set to NULL to prevent accidental reuse