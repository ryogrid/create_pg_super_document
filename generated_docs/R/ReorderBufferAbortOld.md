# ReorderBufferAbortOld

## Location
src/backend/replication/logical/reorderbuffer.c: 3014 - 3060

## Overview
Aborts all transactions that are no longer actually running because the server restarted, cleaning up stale transaction state.

## Definition
```c
void ReorderBufferAbortOld(ReorderBuffer *rb, TransactionId oldestRunningXid)
```

## Detailed Description
ReorderBufferAbortOld is designed to handle cleanup after server crashes or immediate restarts. It aborts transactions that were active before the restart but are no longer running. Unlike ReorderBufferAbort(), this function does not deal with invalidations since these transactions were implicitly aborted due to the server restart.

The function iterates through all toplevel transactions ordered by LSN and aborts any transaction whose XID precedes the oldest currently running transaction ID. The iteration stops at the first transaction that is still alive, as transactions are processed in LSN order and later transactions are likely still valid.

For streamed transactions, the function notifies the remote node about the crash/restart before cleaning up the transaction data.

## Parameters / Member Variables
- `rb`: The ReorderBuffer instance containing the transactions to check
- `oldestRunningXid`: Transaction ID of the oldest transaction currently running; transactions older than this will be aborted

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach_modify
  - dlist_container
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - rbtxn_is_streamed
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
- Called from (representative examples):
  - [standby_decode](../s/standby_decode.md) (in decode.c)

## Notes and Other Information
- Specifically designed for handling transactions after server crashes or immediate restarts
- Does not handle cache invalidations, unlike ReorderBufferAbort()
- Processes transactions in LSN order and stops at the first valid transaction
- For streamed transactions, uses InvalidXLogRecPtr when notifying remote nodes about the abort
- Uses DEBUG2 logging to report aborted transaction IDs
- Optimization: stops iteration once a valid transaction is found, as later transactions are likely still valid