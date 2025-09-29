# ExecutorFinish

## Location
[src/backend/executor/execMain.c:400-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L400-L408)

## Overview
ExecutorFinish must be called after the last ExecutorRun call to perform cleanup operations such as firing AFTER triggers, providing a hook mechanism for plugins while delegating to the standard implementation.

## Definition
```c
void ExecutorFinish(QueryDesc *queryDesc)
```

## Detailed Description
ExecutorFinish performs essential cleanup operations that must occur after query execution is complete but before the executor state is torn down. This function is specifically designed to handle operations like firing AFTER triggers that need to be included in performance measurements for tools like EXPLAIN ANALYZE.

The separation between ExecutorFinish and ExecutorEnd serves an important purpose: ExecutorFinish handles cleanup operations that should be included in total runtime measurements, while ExecutorEnd performs the final resource deallocation that should not be measured. This distinction is crucial for accurate performance analysis.

Like other executor interface functions, ExecutorFinish provides extensibility through the ExecutorFinish_hook variable, allowing loadable plugins to intercept and customize the cleanup process. When no hook is installed, it delegates to standard_ExecutorFinish for the default cleanup behavior.

The function is a critical part of the executor lifecycle: ExecutorStart → ExecutorRun (potentially multiple calls) → ExecutorFinish → ExecutorEnd.

## Parameters / Member Variables
- `queryDesc`: A QueryDesc structure containing the execution context that requires cleanup after execution completion

## Dependencies
- Functions called/Symbols referenced:
  - [standard_ExecutorFinish](../s/standard_ExecutorFinish.md) (default implementation when no hook is present)
  - [QueryDesc](../Q/QueryDesc.md) (parameter structure)
- Called from (representative examples):
  - [EndCopyTo](EndCopyTo.md) (src/backend/commands/copyto.c:731)
  - [ExecCreateTableAs](ExecCreateTableAs.md) (src/backend/commands/createas.c:334)
  - [ExplainOnePlan](ExplainOnePlan.md) (src/backend/commands/explain.c:705)
  - [ProcessQuery](../P/ProcessQuery.md) (src/backend/tcop/pquery.c:193)
  - [PortalCleanup](../P/PortalCleanup.md) (src/backend/commands/portalcmds.c:298)
  - [_SPI_pquery](../S/_SPI_pquery.md) (src/backend/executor/spi.c:2943)

## Notes and Other Information
- Must be called after all ExecutorRun calls are complete but before ExecutorEnd
- Specifically designed to handle cleanup operations that should be included in performance timing measurements
- The hook mechanism allows extensions to perform custom cleanup operations or wrap the standard cleanup behavior
- Critical for proper AFTER trigger execution and other post-execution cleanup tasks
- Separate from ExecutorEnd to distinguish between measured cleanup (ExecutorFinish) and unmeasured resource deallocation (ExecutorEnd)
- Located at src/backend/executor/execMain.c:400-408
- Part of the standard executor lifecycle sequence that ensures proper query execution and cleanup

## Simplified Source

```c
// Simplified version of ExecutorFinish
void ExecutorFinish(QueryDesc *queryDesc) {
    // Check if a custom hook is installed for extensions
    if (ExecutorFinish_hook) {
        // Call the custom hook function
        (*ExecutorFinish_hook)(queryDesc);
    } else {
        // Use the standard PostgreSQL cleanup implementation
        standard_ExecutorFinish(queryDesc);
    }
}
```

Key simplifications made:
- Added descriptive comments to explain the hook mechanism
- Simplified the conditional logic for better readability
- Removed detailed comment block for conciseness while preserving essential information
- Function is already quite simple - main logic is the hook/standard implementation pattern