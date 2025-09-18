# ExecShutdownCustomScan

## Location
src/backend/executor/nodeCustom.c: 221 - 227

## Overview
ExecShutdownCustomScan is a function that performs shutdown operations for custom scan nodes in PostgreSQL's executor, providing a standardized interface for custom scan implementations to clean up resources during parallel query execution shutdown.

## Definition


## Detailed Description
ExecShutdownCustomScan serves as a wrapper function that delegates shutdown operations to custom scan implementations through their method table. This function is part of PostgreSQL's extensibility framework that allows external modules to implement custom scan operators. The function checks if the custom scan implementation provides a ShutdownCustomScan method and calls it if available, ensuring proper cleanup during parallel query execution shutdown phases.

The function follows PostgreSQL's established pattern of providing optional method dispatch - if a custom scan implementation doesn't need special shutdown logic, it can simply omit the ShutdownCustomScan method from its CustomExecMethods structure.

## Parameters / Member Variables
- : A pointer to the CustomScanState structure representing the custom scan node being shut down. This structure contains the execution state and method dispatch table for the custom scan.

## Dependencies
- Functions called/Symbols referenced:
  - [CustomScanState](../C/CustomScanState.md) (structure type)
  - [CustomExecMethods](../C/CustomExecMethods.md) (structure type)
- Called from (representative examples):
  - [ExecShutdownNode_walker](ExecShutdownNode_walker.md) (in src/backend/executor/execProcnode.c:804)

## Notes and Other Information
- This function is part of the optional parallel execution support methods in the CustomExecMethods structure
- The ShutdownCustomScan method pointer may be NULL if the custom scan implementation doesn't require special shutdown logic
- This function is typically called during the cleanup phase of parallel query execution
- Custom scan implementations should use this opportunity to release any resources that were allocated during parallel execution setup
- The function is declared in src/include/executor/nodeCustom.h and implemented in src/backend/executor/nodeCustom.c