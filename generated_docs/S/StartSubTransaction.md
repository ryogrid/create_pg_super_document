# StartSubTransaction

## Location
[src/backend/access/transam/xact.c:5011-5047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5011-L5047)

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

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (type)
  - TRANS_DEFAULT, TRANS_START, TRANS_INPROGRESS (transaction state constants)
  - [TransStateAsString](../T/TransStateAsString.md) (for warning messages)
  - [AtSubStart_Memory](../A/AtSubStart_Memory.md) (memory context initialization)
  - [AtSubStart_ResourceOwner](../A/AtSubStart_ResourceOwner.md) (resource owner initialization)
  - [AfterTriggerBeginSubXact](../A/AfterTriggerBeginSubXact.md) (trigger subsystem initialization)
  - [CallSubXactCallbacks](../C/CallSubXactCallbacks.md) (callback invocation)
  - SUBXACT_EVENT_START_SUB (callback event type)
  - [ShowTransactionState](ShowTransactionState.md) (debugging/logging)
- Called from (representative examples):
  - [CommitTransactionCommandInternal](../C/CommitTransactionCommandInternal.md) (multiple call sites)

## Notes and Other Information
The function includes a detailed comment explaining why it's separate from PushTransaction: it must be called outside of Portal execution context to avoid memory context and resource owner interference. The function includes validation with a WARNING if called from an unexpected transaction state, though this should not occur in normal operation. The initialization order is carefully designed with resource management setup first, followed by subsystem initialization, state transition, and finally callback invocation. This function works in conjunction with the related AtSubStart_Memory and AtSubStart_ResourceOwner functions mentioned in the processed symbol summaries.

## Simplified Source

```c
// Simplified version of StartSubTransaction
static void StartSubTransaction(void) {
    TransactionState s = CurrentTransactionState;

    // Validate current transaction state
    if (s->state != TRANS_DEFAULT)
        elog(WARNING, "StartSubTransaction while in %s state",
             TransStateAsString(s->state));

    // Begin subtransaction startup
    s->state = TRANS_START;

    // Initialize core subsystems for subtransaction
    AtSubStart_Memory();           // Set up memory management
    AtSubStart_ResourceOwner();    // Set up resource ownership
    AfterTriggerBeginSubXact();    // Initialize trigger handling

    // Mark subtransaction as active
    s->state = TRANS_INPROGRESS;

    // Notify registered callbacks about subtransaction start
    CallSubXactCallbacks(SUBXACT_EVENT_START_SUB, s->subTransactionId,
                         s->parent->subTransactionId);

    // Debug output (development builds)
    ShowTransactionState("StartSubTransaction");
}
```

Key simplifications made:
- Added descriptive comments explaining each initialization step
- Preserved the essential state transition logic (DEFAULT → START → INPROGRESS)
- Maintained the critical subsystem initialization order
- Kept the state validation warning for debugging
- Simplified callback invocation with clearer parameter meaning
- Retained the debug output call but marked it as development-only