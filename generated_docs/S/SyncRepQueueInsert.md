# SyncRepQueueInsert

## Location
[src/backend/replication/syncrep.c:372-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L372-L405)

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

## Simplified Source

```c
static void
SyncRepQueueInsert(int mode)
{
    dlist_head *queue;
    dlist_iter iter;

    // Validate mode and get appropriate queue
    Assert(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);
    queue = &WalSndCtl->SyncRepQueue[mode];

    // Search backwards from tail to find insertion point
    dlist_reverse_foreach(iter, queue) {
        PGPROC *proc = dlist_container(PGPROC, syncRepLinks, iter.cur);

        // Insert after first process with smaller LSN
        if (proc->waitLSN < MyProc->waitLSN) {
            dlist_insert_after(&proc->syncRepLinks, &MyProc->syncRepLinks);
            return;
        }
    }

    // Empty queue or smallest LSN - insert at head
    dlist_push_head(queue, &MyProc->syncRepLinks);
}
```