# ExecutorEnd

## Location
src/backend/executor/execMain.c: 460 - 468

## Overview
A hook-enabled wrapper function that must be called at the end of execution of any query plan, providing plugin extensibility for executor cleanup operations.

## Definition


## Detailed Description
The  function serves as the primary entry point for ending query execution in PostgreSQL. It implements a plugin hook mechanism that allows loadable extensions to intercept and customize the executor end process. If no hook is installed, it delegates to the standard implementation. This design pattern enables third-party plugins to perform custom cleanup, logging, or monitoring operations while maintaining the standard executor behavior.

The function is a critical part of the executor lifecycle and must be called for every query plan execution to ensure proper resource cleanup and state management.

## Parameters / Member Variables
- : Pointer to the QueryDesc structure containing the query execution context and estate information

## Dependencies
- Functions called/Symbols referenced:
  - ExecutorEnd_hook (function pointer, may be NULL)
  - standard_ExecutorEnd
- Called from (representative examples):
  - EndCopyTo
  - ExecCreateTableAs
  - ExplainOnePlan
  - ProcessQuery
  - _SPI_pquery
  - PortalCleanup

## Notes and Other Information
- Provides a hook mechanism (ExecutorEnd_hook) for plugin extensibility
- Must be called at the end of execution of any query plan
- Plugins using the hook should normally call standard_ExecutorEnd() to maintain standard behavior
- Used extensively throughout the PostgreSQL codebase for various query execution contexts
- Essential for proper resource cleanup and executor state management