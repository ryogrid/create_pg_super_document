# AtEOXact_SPI

## Location
src/backend/executor/spi.c: 428 - 481

## Overview
AtEOXact_SPI cleans up SPI state at transaction commit or abort, handling proper cleanup of nested SPI connections when a transaction ends.

## Definition
```c
void AtEOXact_SPI(bool isCommit)
```

## Detailed Description
This function is called at the end of a transaction (either commit or abort) to clean up any remaining SPI connections that were not properly closed with SPI_finish(). It pops SPI stack entries until it finds one marked as internal_xact (belonging to SPI_commit/SPI_rollback caller) or reaches the bottom of the stack.

The function restores the outer global variables (SPI_processed, SPI_tuptable, SPI_result) for each connection being cleaned up. Memory contexts are automatically cleaned up by the transaction cleanup process.

During a commit, if any SPI connections are found that need cleanup, a WARNING is issued suggesting missing SPI_finish() calls, as this indicates a potential programming error.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether this is being called during transaction commit (true) or abort (false)

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_connection (struct type)
  - _SPI_connected (global variable)
  - _SPI_stack (global array)
  - _SPI_current (global variable)
  - SPI_processed (global variable)
  - SPI_tuptable (global variable)
  - SPI_result (global variable)
  - ereport (error reporting function)

- Called from (representative examples):
  - CommitTransaction (src/backend/access/transam/xact.c:2412)
  - PrepareTransaction (src/backend/access/transam/xact.c:2701)
  - AbortTransaction (src/backend/access/transam/xact.c:2921)

## Notes and Other Information
- This function is part of PostgreSQL's transaction cleanup infrastructure
- It only issues warnings during commit; during abort, cleanup is expected
- Memory contexts are automatically freed by the transaction system
- The function stops when it encounters an internal_xact connection, preserving SPI connections created by SPI_commit/SPI_rollback operations
- Located in src/backend/executor/spi.c:428-481