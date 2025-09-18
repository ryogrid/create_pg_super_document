# SyncRepQueueInsert

## Location
src/backend/replication/syncrep.c: 372 - 405

## Overview
Inserts the current process (MyProc) into the appropriate synchronous replication wait queue while maintaining the sorted order by LSN (Log Sequence Number).

## Definition
```c
static void SyncRepQueueInsert(int mode)
```

## Detailed Description
This static function maintains the ordering invariant of the synchronous replication wait queues by inserting processes in LSN order. The function implements an efficient insertion algorithm that starts at the tail of the queue and works backwards to find the correct insertion point, taking advantage of the common case where new processes typically have higher LSNs and belong at the end of the queue.

The function supports multiple synchronous replication modes (such as remote_write, remote_flush, and remote_apply) by accepting a mode parameter that selects the appropriate queue. Each mode has its own separate queue to track processes waiting for different levels of replication confirmation.

When inserting, the function compares the current process's waitLSN with existing queue entries, placing the process after any entry with a smaller LSN. If no such entry is found (empty queue or lowest LSN), the process is inserted at the head of the queue.

## Parameters / Member Variables
- `mode`: Integer specifying which synchronous replication queue to use (corresponds to sync rep wait modes like remote_write, remote_flush, remote_apply)

## Dependencies
- Functions called/Symbols referenced:
  - dlist_reverse_foreach (doubly-linked list reverse iteration)
  - dlist_container (extracts PGPROC from list node)
  - [dlist_insert_after](../d/dlist_insert_after.md) (inserts node after specified position)
  - [dlist_push_head](../d/dlist_push_head.md) (inserts node at queue head)
  - [PGPROC](../P/PGPROC.md) (process structure type)
  - NUM_SYNC_REP_WAIT_MODE (maximum number of sync rep modes)
- Called from (representative examples):
  - [SyncRepWaitForLSN](SyncRepWaitForLSN.md)
  - SyncStandbysDefined

## Notes and Other Information
The function assumes that MyProc->waitLSN has been set before insertion and that the caller holds the appropriate SyncRepLock. The reverse iteration strategy optimizes for the common case where processes arrive in LSN order, making insertion at the tail most frequent. The function maintains the critical invariant that each queue remains sorted by LSN, which is essential for WAL senders to efficiently process replication confirmations in order.