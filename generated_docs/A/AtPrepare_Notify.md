# AtPrepare_Notify

## Location
src/backend/commands/async.c: 836 - 860

## Overview
A transaction preparation phase hook that prevents two-phase transactions from being prepared if they contain any pending LISTEN, UNLISTEN, or NOTIFY operations.

## Definition
```c
void AtPrepare_Notify(void)
```

## Detailed Description
This function is called during the prepare phase of two-phase commit transactions (prepared transactions) to validate that the transaction doesn't contain any notification-related operations. PostgreSQL doesn't support persisting LISTEN/UNLISTEN/NOTIFY state across transaction boundaries in prepared transactions, as these operations affect session-local state and shared notification queues that cannot be easily rolled back or committed in a distributed manner.

The function checks two global variables: `pendingActions` (which tracks pending LISTEN/UNLISTEN operations) and `pendingNotifies` (which tracks pending NOTIFY operations). If either contains pending operations, the function raises an ERROR, preventing the transaction from being prepared.

## Parameters / Member Variables
- No input parameters
- Returns: `void` (function may not return if error is raised)

## Dependencies
- Functions called/Symbols referenced:
  - `ereport()` - PostgreSQL error reporting function (implicitly used)
  - `pendingActions` - Global variable tracking pending LISTEN/UNLISTEN operations
  - `pendingNotifies` - Global variable tracking pending NOTIFY operations
  - `ERRCODE_FEATURE_NOT_SUPPORTED` - PostgreSQL error code constant
- Called from:
  - `PrepareTransaction()` - Main transaction preparation function
  - Referenced in `src/include/commands/async.h` - Header file declaration

## Notes and Other Information
- This function enforces a fundamental limitation of PostgreSQL's two-phase commit protocol
- LISTEN/UNLISTEN operations affect session-local state that cannot be meaningfully prepared
- NOTIFY operations interact with shared queues in ways that are incompatible with two-phase commit
- The restriction ensures data consistency and prevents complex edge cases in distributed transactions
- Location: src/backend/commands/async.c:836-860
- Part of PostgreSQL's transaction management system for notification operations