# finish_lo_xact

## Location
src/bin/psql/large_obj.c: 98 - 120

## Overview
Handles the successful completion of large object operations by committing transactions that were started by the operation, ensuring proper transaction cleanup in psql.

## Definition
```c
static bool finish_lo_xact(const char *operation, bool own_transaction)
```

## Detailed Description
This function serves as the cleanup counterpart to start_lo_xact, handling the successful completion of large object operations. It commits transactions that were explicitly started for the large object operation while respecting psql's autocommit setting. The function only commits transactions that it owns (as indicated by the own_transaction parameter), leaving externally-managed transactions untouched. If a commit fails, it attempts to rollback the transaction to maintain database consistency.

## Parameters / Member Variables
- `operation`: String describing the operation being completed (used for context, though not directly used in current implementation)
- `own_transaction`: Boolean indicating whether this function should commit the transaction (true if start_lo_xact started it)

## Dependencies
- Functions called/Symbols referenced:
  - PSQLexec (psql utility function to execute SQL commands)
  - PQclear (libpq function to free result memory)
  - pset.autocommit (global setting controlling automatic transaction commits)
- Called from (representative examples):
  - do_lo_export
  - do_lo_import
  - do_lo_unlink

## Notes and Other Information
- Returns true on success, false if commit failed
- Only commits transactions when both own_transaction is true AND autocommit is enabled
- Implements fallback rollback mechanism if commit fails
- Designed to work in tandem with start_lo_xact for proper transaction lifecycle management
- Static function with scope limited to large_obj.c file
- Critical for maintaining transaction integrity in large object operations