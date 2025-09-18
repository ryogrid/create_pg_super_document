# StartSubTransaction

## Location
src/backend/access/transam/xact.c: 5011 - 5047

## Overview
StartSubTransaction initializes and starts a new subtransaction by setting up the necessary subsystems and transitioning the transaction state from TRANS_DEFAULT to TRANS_INPROGRESS.

## Definition
```c
static void StartSubTransaction(void)
```

## Detailed Description
StartSubTransaction is responsible for the complete initialization of a new subtransaction after the preliminary setup has been done by DefineSavepoint. The function is designed to be called during the main idle loop via CommitTransactionCommand, rather than directly from DefineSavepoint, to avoid issues with memory context and resource owner management within Portal execution.

The function performs a comprehensive initialization sequence: it validates the current transaction state, transitions through TRANS_START to TRANS_INPROGRESS, initializes memory management and resource ownership for the subtransaction, sets up trigger handling, and invokes subtransaction start callbacks. This careful orchestration ensures that all PostgreSQL subsystems are properly prepared for subtransaction operations.

## Parameters / Member Variables
This function takes no parameters and has no return value. It operates on the global CurrentTransactionState.

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (type)
  - TRANS_DEFAULT, TRANS_START, TRANS_INPROGRESS (transaction state constants)
  - TransStateAsString (for warning messages)
  - AtSubStart_Memory (memory context initialization)
  - AtSubStart_ResourceOwner (resource owner initialization)
  - AfterTriggerBeginSubXact (trigger subsystem initialization)
  - CallSubXactCallbacks (callback invocation)
  - SUBXACT_EVENT_START_SUB (callback event type)
  - ShowTransactionState (debugging/logging)
- Called from (representative examples):
  - CommitTransactionCommandInternal (multiple call sites)

## Notes and Other Information
The function includes a detailed comment explaining why it's separate from PushTransaction: it must be called outside of Portal execution context to avoid memory context and resource owner interference. The function includes validation with a WARNING if called from an unexpected transaction state, though this should not occur in normal operation. The initialization order is carefully designed with resource management setup first, followed by subsystem initialization, state transition, and finally callback invocation. This function works in conjunction with the related AtSubStart_Memory and AtSubStart_ResourceOwner functions mentioned in the processed symbol summaries.