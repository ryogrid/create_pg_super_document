# BeginTransactionBlock

## Location
[src/backend/access/transam/xact.c:3873-3940](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3873-L3940)

## Overview
BeginTransactionBlock implements the SQL BEGIN command by transitioning the current transaction state to begin an explicit transaction block, with validation to prevent invalid state transitions.

## Definition


## Detailed Description
This function handles the execution of a BEGIN command by examining the current transaction block state and performing appropriate state transitions. It supports starting new transaction blocks from TBLOCK_STARTED state and converting implicit transaction blocks (TBLOCK_IMPLICIT_INPROGRESS) to explicit ones. The function includes comprehensive validation logic that issues warnings for redundant BEGIN commands when already in a transaction block, and fatal errors for invalid state transitions. The state machine design ensures proper transaction block lifecycle management and prevents inconsistent transaction states.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type definition)
  - CurrentTransactionState (global variable)
  - TBlockState enumeration values (TBLOCK_STARTED, TBLOCK_BEGIN, etc.)
  - ereport (error reporting function)
  - elog (logging function)
  - [BlockStateAsString](BlockStateAsString.md) (utility function for debugging)
  - [errcode](../e/errcode.md) (error code definition)
  - [errmsg](../e/errmsg.md) (error message formatting)
- Called from (representative examples):
  - [pa_start_subtrans](../p/pa_start_subtrans.md) (at src/backend/replication/logical/applyparallelworker.c:1381)
  - [apply_handle_prepare_internal](../a/apply_handle_prepare_internal.md) (at src/backend/replication/logical/worker.c:1092)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (at src/backend/tcop/utility.c:609)

## Notes and Other Information
- Supports both new transaction blocks and conversion of implicit transactions to explicit ones
- Issues WARNING (not ERROR) when BEGIN is called within an existing transaction block, maintaining compatibility
- Uses comprehensive state validation with FATAL errors for truly invalid states
- State transition logic follows PostgreSQL's transaction block state machine design
- Called primarily from utility command processing and replication worker contexts
- Historical compatibility maintained for allowing BEGIN within implicit transaction blocks