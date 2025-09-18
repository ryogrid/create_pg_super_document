# AtCommit_Notify

## Location
src/backend/commands/async.c: 968 - 1040

## Overview
A post-commit hook that finalizes LISTEN/UNLISTEN operations, signals listening backends about new notifications, and cleans up transaction-local notification state after transaction commit.

## Definition
```c
void AtCommit_Notify(void)
```

## Detailed Description
This function is called at transaction commit, after the transaction has been committed to the commit log (clog). It performs the final phase of notification processing by executing all pending LISTEN/UNLISTEN actions, signaling backends about queued notifications, and cleaning up transaction state. 

The function operates in several phases:
1. **Execute Pending Actions**: Processes all queued LISTEN/UNLISTEN operations by calling the appropriate commit functions
2. **Backend Management**: Unregisters the backend from the listener array if it's no longer listening to any channels
3. **Notification Delivery**: Signals listening backends about notifications that were queued during PreCommit_Notify
4. **Queue Maintenance**: Optionally advances the global queue tail pointer to free up processed messages
5. **Cleanup**: Clears all transaction-local pending actions and notifications

The function ensures that notification operations are atomic with the transaction and that all listening backends are properly notified of new messages.

## Parameters / Member Variables
- No input parameters
- Returns: `void`

## Dependencies
- Functions called/Symbols referenced:
  - `elog()` - Logging function with DEBUG1 level
  - `[ListenAction](../L/ListenAction.md)` - Structure representing queued LISTEN/NOTIFY operations
  - `LISTEN_LISTEN`, `LISTEN_UNLISTEN`, `LISTEN_UNLISTEN_ALL` - Action type constants
  - `[Exec_ListenCommit](../E/Exec_ListenCommit.md)()` - Adds channel to backend's listen list
  - `[Exec_UnlistenCommit](../E/Exec_UnlistenCommit.md)()` - Removes specific channel from listen list
  - `[Exec_UnlistenAllCommit](../E/Exec_UnlistenAllCommit.md)()` - Removes all channels from listen list
  - `[asyncQueueUnregister](../a/asyncQueueUnregister.md)()` - Removes backend from shared listener array
  - `[SignalBackends](../S/SignalBackends.md)()` - Signals listening backends about new notifications
  - `[asyncQueueAdvanceTail](../a/asyncQueueAdvanceTail.md)()` - Advances global queue tail pointer
  - `[ClearPendingActionsAndNotifies](../C/ClearPendingActionsAndNotifies.md)()` - Cleans up transaction-local state
  - `amRegisteredListener`, `listenChannels` - Global state variables
  - `pendingActions`, `pendingNotifies` - Global pending operation lists
  - `tryAdvanceTail` - Global flag indicating whether to try advancing tail
- Called from:
  - `[CommitTransaction](../C/CommitTransaction.md)()` - Main transaction commit function
  - Referenced in `src/include/commands/async.h` - Header file declaration

## Notes and Other Information
- This function complements PreCommit_Notify by handling the post-commit phase
- [Backend](../B/Backend.md) signaling occurs only if there were pending notifications
- Queue tail advancement is done by senders to reduce contention
- The function handles both successful commit cleanup and ensures proper resource management
- All pending actions are processed atomically as part of the transaction commit
- Location: src/backend/commands/async.c:968-1040
- Critical for completing the two-phase notification commit protocol