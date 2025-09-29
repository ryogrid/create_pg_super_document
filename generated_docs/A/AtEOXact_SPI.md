# AtEOXact_SPI

## Location
[src/backend/executor/spi.c:428-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L428-L481)

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
  - [CommitTransaction](../C/CommitTransaction.md) (src/backend/access/transam/xact.c:2412)
  - [PrepareTransaction](../P/PrepareTransaction.md) (src/backend/access/transam/xact.c:2701)
  - [AbortTransaction](AbortTransaction.md) (src/backend/access/transam/xact.c:2921)

## Notes and Other Information
- This function is part of PostgreSQL's transaction cleanup infrastructure
- It only issues warnings during commit; during abort, cleanup is expected
- Memory contexts are automatically freed by the transaction system
- The function stops when it encounters an internal_xact connection, preserving SPI connections created by SPI_commit/SPI_rollback operations
- Located in src/backend/executor/spi.c:428-481

## Simplified Source

```c
// Simplified version of AtEOXact_SPI
void AtEOXact_SPI(bool isCommit) {
    bool found = false;

    // Pop SPI stack entries until we find an internal transaction marker
    while (_SPI_connected >= 0) {
        _SPI_connection *connection = &(_SPI_stack[_SPI_connected]);

        // Stop at internal transaction markers (from SPI_commit/SPI_rollback)
        if (connection->internal_xact)
            break;

        found = true;

        // Restore outer global SPI state variables
        SPI_processed = connection->outer_processed;
        SPI_tuptable = connection->outer_tuptable;
        SPI_result = connection->outer_result;

        // Move to previous stack level
        _SPI_connected--;
        if (_SPI_connected < 0)
            _SPI_current = NULL;
        else
            _SPI_current = &(_SPI_stack[_SPI_connected]);
    }

    // Warn if SPI connections were left open during commit
    if (found && isCommit) {
        ereport(WARNING,
                (errcode(ERRCODE_WARNING),
                 errmsg("transaction left non-empty SPI stack"),
                 errhint("Check for missing \"SPI_finish\" calls.")));
    }
}
```

Key simplifications made:
- Streamlined the main loop logic for better readability
- Consolidated comments to focus on core functionality
- Preserved essential error handling and warning logic
- Maintained the critical stack unwinding algorithm
- Removed redundant comments while keeping essential documentation