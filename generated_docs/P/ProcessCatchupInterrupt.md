# ProcessCatchupInterrupt

## Location
src/backend/storage/ipc/sinval.c: 175 - 210

## Overview
ProcessCatchupInterrupt handles the actual processing of shared invalidation catchup events that were deferred from the signal handler context.

## Definition
```c
void ProcessCatchupInterrupt(void)
```

## Detailed Description
ProcessCatchupInterrupt is the complementary function to HandleCatchupInterrupt that performs the actual work of processing shared invalidation catchup events. While HandleCatchupInterrupt runs in signal handler context and can only set flags, this function runs in normal execution context where it's safe to perform complex database operations.

The function operates in a loop checking the catchupInterruptPending flag. When catchup processing is needed, it ensures that ReceiveSharedInvalidMessages() gets called to process pending invalidation messages. The implementation carefully handles two different execution contexts:

1. **Inside a transaction**: Simply calls AcceptInvalidationMessages() directly, which will process the invalidations and reset the catchupInterruptPending flag.

2. **Outside a transaction**: Creates a minimal transaction (StartTransactionCommand/CommitTransactionCommand cycle) to provide the necessary transaction context for invalidation processing. The AcceptInvalidationMessages() call happens automatically during transaction start. It carefully preserves the caller's memory context since transaction contexts are temporary.

The function includes extensive comments explaining why the transaction wrapper is necessary for proper error cleanup, even though it might seem like overkill for simple invalidation processing.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - IsTransactionOrTransactionBlock (checks current transaction state)
  - AcceptInvalidationMessages (processes invalidation messages when inside transaction)
  - StartTransactionCommand (begins transaction when outside transaction context)
  - CommitTransactionCommand (commits transaction when outside transaction context)  
  - MemoryContextIsValid (validates memory context preservation)
  - catchupInterruptPending (global flag variable, checked in loop)
  - CurrentMemoryContext (current memory context, preserved across transaction)
- Called from (representative examples):
  - HandleAutoVacLauncherInterrupts (in autovacuum launcher)
  - ProcessClientReadInterrupt (in backend client read processing)

## Notes and Other Information
- This function runs outside of signal handler context, unlike HandleCatchupInterrupt
- Uses a while loop to handle multiple pending catchup events that may accumulate
- The transaction wrapper when outside transaction context ensures proper error handling and cleanup
- Memory context preservation is critical to avoid invalidating caller's data structures
- Debug logging at DEBUG4 level helps trace catchup processing in different contexts
- Part of PostgreSQL's shared invalidation system that maintains cache consistency across backends
- The function is designed to be safe to call from various execution contexts within the backend