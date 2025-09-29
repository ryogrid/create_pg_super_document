# PreventCommandIfParallelMode

## Location
[src/backend/tcop/utility.c:422-440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L422-L440)

## Overview
PreventCommandIfParallelMode throws an error if the current subtransaction is in parallel mode, ensuring commands incompatible with parallel execution are properly rejected.

## Definition
`void PreventCommandIfParallelMode(const char *cmdname)`

## Detailed Description
This function provides a centralized mechanism for detecting and preventing the execution of commands during parallel operations. It checks whether the current subtransaction is operating in parallel mode using IsInParallelMode() and raises a standardized error if parallel mode is active.

The function ensures consistency in error message wording across the PostgreSQL codebase. Some callers may have already performed the IsInParallelMode() check themselves but still require the standardized error reporting functionality. Parallel mode restrictions exist because certain operations rely on backend-local state that may not be properly synchronized across parallel workers.

## Parameters / Member Variables
- `cmdname`: String containing the name of the SQL command being executed (e.g., "CREATE", "SET") for inclusion in the error message

## Dependencies
- Functions called/Symbols referenced:
  - [IsInParallelMode](../I/IsInParallelMode.md) (function to check if currently in parallel mode)
  - ereport (error reporting mechanism)
  - [errcode](../e/errcode.md), errmsg (error handling macros)
- Called from (representative examples):
  - [nextval_internal](../n/nextval_internal.md) (src/backend/commands/sequence.c:666)
  - [do_setval](../d/do_setval.md) (src/backend/commands/sequence.c:983)
  - [ExecCheckXactReadOnly](../E/ExecCheckXactReadOnly.md) (src/backend/executor/execMain.c:814)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (src/backend/tcop/utility.c:580)

## Notes and Other Information
- Uses ERRCODE_INVALID_TRANSACTION_STATE error code for parallel mode violations
- Primarily used in sequence operations and utility command processing where backend-local state synchronization is critical
- Part of PostgreSQL's parallel query execution safety framework
- Helps maintain data consistency by preventing operations that could lead to undefined behavior in parallel contexts

## Simplified Source

```c
// Prevent command execution during parallel operations
void
PreventCommandIfParallelMode(const char *cmdname)
{
    if (IsInParallelMode())
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                 errmsg("cannot execute %s during a parallel operation",
                        cmdname)));
}
```