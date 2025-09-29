# ExecutorEnd

## Location
[src/backend/executor/execMain.c:460-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L460-L468)

## Overview
A hook-enabled wrapper function that must be called at the end of execution of any query plan, providing plugin extensibility for executor cleanup operations.

## Definition

```c
void
ExecutorEnd(QueryDesc *queryDesc)
```
## Detailed Description
The  function serves as the primary entry point for ending query execution in PostgreSQL. It implements a plugin hook mechanism that allows loadable extensions to intercept and customize the executor end process. If no hook is installed, it delegates to the standard implementation. This design pattern enables third-party plugins to perform custom cleanup, logging, or monitoring operations while maintaining the standard executor behavior.

The function is a critical part of the executor lifecycle and must be called for every query plan execution to ensure proper resource cleanup and state management.

## Parameters / Member Variables
- : Pointer to the QueryDesc structure containing the query execution context and estate information

## Dependencies
- Functions called/Symbols referenced:
  - ExecutorEnd_hook (function pointer, may be NULL)
  - [standard_ExecutorEnd](../s/standard_ExecutorEnd.md)
- Called from (representative examples):
  - [EndCopyTo](EndCopyTo.md)
  - [ExecCreateTableAs](ExecCreateTableAs.md)
  - [ExplainOnePlan](ExplainOnePlan.md)
  - [ProcessQuery](../P/ProcessQuery.md)
  - [_SPI_pquery](../S/_SPI_pquery.md)
  - [PortalCleanup](../P/PortalCleanup.md)

## Notes and Other Information
- Provides a hook mechanism (ExecutorEnd_hook) for plugin extensibility
- Must be called at the end of execution of any query plan
- Plugins using the hook should normally call standard_ExecutorEnd() to maintain standard behavior
- Used extensively throughout the PostgreSQL codebase for various query execution contexts
- Essential for proper resource cleanup and executor state management

## Simplified Source

```c
// Simplified version of ExecutorEnd
void ExecutorEnd(QueryDesc *queryDesc)
{
    // Check if a plugin has installed a custom hook
    if (ExecutorEnd_hook) {
        // Call the custom hook function
        (*ExecutorEnd_hook)(queryDesc);
    } else {
        // Use the standard PostgreSQL implementation
        standard_ExecutorEnd(queryDesc);
    }
}
```

Key simplifications made:
- Added descriptive comments explaining the hook pattern
- Clarified the conditional logic flow
- Preserved the essential plugin extensibility mechanism
- Function is already minimal - no major simplifications needed