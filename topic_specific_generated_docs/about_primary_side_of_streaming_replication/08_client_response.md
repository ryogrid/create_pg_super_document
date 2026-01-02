# Chapter 8: Client Response and Commit Completion

<- [Previous: Sync Wait/Release](07_sync_wait_release.md) | [Index](index.md)

---

## Overview

This chapter covers the final phase of synchronous replication - when the backend wakes up after synchronous replication confirmation and returns control to the client. This completes the commit cycle, ensuring the transaction is durably replicated before the client receives acknowledgment.

**Related Diagrams:**
- [Figure 11: Complete Commit Sequence](diagrams/11_complete_commit_sequence.mermaid) - End-to-end commit flow

---

## Processing Flow

The commit completion flow:

```
Backend in SyncRepWaitForLSN wait loop
    |
    +---> Walsender calls SetLatch(backend)
    |
    +---> WaitLatch() returns
    |
    +---> Check syncRepState == SYNC_REP_WAIT_COMPLETE
    |
    +---> pg_read_barrier() -----> Ensure visibility of updates
    |
    +---> Reset sync rep state variables
    |
    +---> Return from SyncRepWaitForLSN()
    |
    v
Back in RecordTransactionCommit()
    |
    +---> Continue commit processing
    |
    +---> Release locks
    |
    +---> Reset transaction state
    |
    v
Return to client
    |
    +---> Send COMMIT acknowledgment
```

---

## Implementation Details

### Wake and Completion in SyncRepWaitForLSN

**Location:** `src/backend/replication/syncrep.c:271-363`

```c
// syncrep.c - End of wait loop in SyncRepWaitForLSN
for (;;)
{
    int rc;

    ResetLatch(MyLatch);

    /*
     * Acquiring the lock is not needed, the latch ensures proper
     * barriers. If it looks like we're done, we must really be done,
     * because once walsender changes the state to SYNC_REP_WAIT_COMPLETE,
     * it will never update it again.
     */
    if (MyProc->syncRepState == SYNC_REP_WAIT_COMPLETE)
        break;

    // ... interrupt handling ...

    rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
                   WAIT_EVENT_SYNC_REP);

    // ... postmaster death handling ...
}

/*
 * WalSender has checked our LSN and has removed us from queue. Clean up
 * state and leave. It's OK to reset these shared memory fields without
 * holding SyncRepLock, because any walsenders will ignore us anyway when
 * we're not on the queue.
 */
pg_read_barrier();
Assert(dlist_node_is_detached(&MyProc->syncRepLinks));
MyProc->syncRepState = SYNC_REP_NOT_WAITING;
MyProc->waitLSN = 0;

/* reset ps display to remove the suffix */
if (update_process_title)
    set_ps_display_remove_suffix();
```

### Memory Barrier Requirement

The `pg_read_barrier()` after the loop ensures:

1. We see all updates the walsender made before setting `SYNC_REP_WAIT_COMPLETE`
2. The queue link detachment is visible
3. Any other shared state changes are visible

**Cross-reference:** See [Chapter 7](07_sync_wait_release.md#memory-barrier-requirements) for the corresponding write barrier in `SyncRepWakeQueue()`.

---

### Commit Path Integration

**Location:** `src/backend/access/transam/xact.c` (RecordTransactionCommit)

The sync rep wait is integrated into the commit path:

```c
// Simplified from xact.c - RecordTransactionCommit
static TransactionId
RecordTransactionCommit(void)
{
    XLogRecPtr commitLSN;

    /* Insert commit record */
    commitLSN = XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT);

    /* Flush WAL to disk */
    XLogFlush(commitLSN);

    /* Wait for synchronous replication */
    if (synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_WRITE)
    {
        SyncRepWaitForLSN(commitLSN, true);
    }

    /* Mark transaction committed in clog */
    TransactionIdCommitTree(xid, nchildren, children);

    /* Return committed XID */
    return xid;
}
```

**Key ordering:**
1. Insert commit WAL record -> LSN assigned
2. Flush WAL to local disk -> Durable locally
3. Wait for sync rep -> Durable on standby(s)
4. Mark in clog -> Visible to other transactions

---

### Interrupt Handling During Wait

If the backend receives an interrupt while waiting:

```c
// From SyncRepWaitForLSN
if (ProcDiePending)
{
    ereport(WARNING,
            (errcode(ERRCODE_ADMIN_SHUTDOWN),
             errmsg("canceling the wait for synchronous replication and "
                    "terminating connection due to administrator command"),
             errdetail("The transaction has already committed locally, "
                       "but might not have been replicated to the standby.")));
    whereToSendOutput = DestNone;
    SyncRepCancelWait();
    break;
}

if (QueryCancelPending)
{
    QueryCancelPending = false;
    ereport(WARNING,
            (errmsg("canceling wait for synchronous replication due to user request"),
             errdetail("The transaction has already committed locally, "
                       "but might not have been replicated to the standby.")));
    SyncRepCancelWait();
    break;
}
```

**Key points:**

| Aspect | Behavior |
|--------|----------|
| Transaction status | Already committed locally (cannot be aborted) |
| Error level | WARNING, not ERROR (to avoid confusing application) |
| Client communication | `whereToSendOutput = DestNone` prevents further messages |
| Backend continuation | Process continues (for shutdown cleanup) |

---

### SyncRepCancelWait Function

**Location:** `src/backend/replication/syncrep.c:405`

```c
void
SyncRepCancelWait(void)
{
    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

    if (!dlist_node_is_detached(&MyProc->syncRepLinks))
        dlist_delete_thoroughly(&MyProc->syncRepLinks);

    MyProc->syncRepState = SYNC_REP_NOT_WAITING;

    LWLockRelease(SyncRepLock);
}
```

This properly removes the backend from the queue and resets state, ensuring clean abort of the wait.

---

### Return to Client

After `SyncRepWaitForLSN()` returns (normally or via interrupt):

```c
// Back in transaction commit code
void
CommitTransaction(void)
{
    // ... earlier commit processing ...

    RecordTransactionCommit();  // Includes sync rep wait

    // Release all locks
    ProcReleaseLocks(true);

    // Reset transaction state
    s->state = TRANS_DEFAULT;

    // Return to command processing
    // (EndCommand will send response to client)
}
```

The client receives the commit confirmation only after all this completes.

---

## Complete Transaction Timeline

```
Time    Primary Backend          Walsender              Standby
----    ---------------          ---------              -------
T1      COMMIT received
T2      XLogInsert(COMMIT)
T3      XLogFlush()
T4      SyncRepWaitForLSN()
T5      Insert into queue
T6      WaitLatch()
                                 (woken by CV)
T7                               XLogSendPhysical()
T8                               Send WAL              Receive WAL
T9                                                     Write to disk
T10                                                    Flush (fsync)
T11                                                    Send reply
T12                              ProcessStandbyReplyMessage()
T13                              SyncRepReleaseWaiters()
T14                              SyncRepWakeQueue()
T15                              SetLatch(backend)
T16     Wake from latch
T17     Verify WAIT_COMPLETE
T18     Reset state
T19     Release locks
T20     Send COMMIT to client
        ----- Client receives confirmation -----
```

---

## Error Scenarios

### Standby Disconnect During Wait

If the standby disconnects while backends are waiting:

1. Walsender detects connection failure
2. Walsender exits (does not release waiters)
3. If another sync standby takes over, its walsender will release waiters
4. If no sync standbys remain, `SyncRepUpdateSyncStandbysDefined()` releases all

### Primary Shutdown During Wait

If the postmaster initiates shutdown:

1. `WL_POSTMASTER_DEATH` event triggers
2. Backend sets `ProcDiePending = true`
3. Wait is canceled with warning
4. Backend proceeds with shutdown

### Timeout Scenarios

There is **no explicit timeout** for sync rep wait. The wait continues indefinitely until:

1. Walsender confirms the LSN
2. Interrupt (SIGTERM, query cancel)
3. Postmaster death

**Recommendation:** Applications should use `statement_timeout` if bounded wait is required.

---

## Configuration Parameters

| Parameter | Default | Impact |
|-----------|---------|--------|
| `synchronous_commit` | `on` | Determines if/how long to wait |
| `statement_timeout` | `0` | Can limit overall statement duration including wait |

**Cross-reference:** See [Appendix C: Configuration Parameters](appendix_config_params.md) for complete documentation.

---

## Performance Considerations

### Sync Rep Latency Components

| Component | Typical Time | Description |
|-----------|--------------|-------------|
| Network RTT | 0.1-100ms | Network round trip to standby |
| Standby Write | 0.1-1ms | Write to standby WAL buffer |
| Standby Flush | 1-10ms | Fsync on standby |
| Reply Processing | <0.1ms | Walsender processing |
| Wakeup | <0.1ms | Latch set and context switch |

**Total typical latency:** Network RTT + standby flush time (1-110ms)

### Optimizing Sync Rep Latency

| Optimization | Impact |
|--------------|--------|
| Low-latency network | Reduces RTT component |
| Fast standby storage | Reduces flush time |
| Lower `wal_receiver_status_interval` | Faster confirmation replies |
| Group commit | Multiple transactions share one round-trip |

---

## Diagrams

### Figure 11: Complete Commit Sequence

**Location:** [diagrams/11_complete_commit_sequence.mermaid](diagrams/11_complete_commit_sequence.mermaid)

This diagram shows the entire flow from COMMIT to client response, including all eight phases of synchronous replication.

---

## Key Takeaways

1. **Transaction committed before wait:** The transaction is committed locally before entering sync rep wait. It cannot be aborted - interrupts only skip the wait with a warning.

2. **Memory barriers ensure visibility:** `pg_read_barrier()` after wake ensures the backend sees all walsender's updates. The corresponding `pg_write_barrier()` is in `SyncRepWakeQueue()`.

3. **Lock-free state check:** The state check (`syncRepState == WAIT_COMPLETE`) is lock-free because the latch provides the necessary memory barrier.

4. **No explicit timeout:** Sync rep wait has no timeout. Use `statement_timeout` for bounded wait. This design ensures transactions don't silently lose durability guarantees.

5. **Client only sees success after confirmation:** The COMMIT acknowledgment is sent only after full sync rep confirmation, providing the durability guarantee to the application.

6. **Failover handling:** If a sync standby fails, another standby (if configured) can take over and release waiters. Complete sync standby failure requires intervention.

7. **Warning on interrupt:** Interrupts result in WARNING messages, not errors. The transaction is already committed - the message informs the client about the potential replication gap.

---

## Related Sections

- **Previous:** [Chapter 7: Sync Wait/Release](07_sync_wait_release.md) - Wait mechanics
- **Full Flow:** [Figure 11: Complete Commit Sequence](diagrams/11_complete_commit_sequence.mermaid)
- **Index:** [Documentation Index](index.md)

---

## Complete Documentation Navigation

### Chapters

1. [Architecture Overview](01_architecture_overview.md)
2. [WAL Generation and LSN Assignment](02_wal_generation_lsn.md)
3. [WAL Persistence](03_wal_persistence.md)
4. [Walsender Transmission](04_walsender_transmission.md)
5. [Keepalive and Monitoring](05_keepalive_monitoring.md)
6. [Standby Response Processing](06_standby_response.md)
7. [Synchronous Replication Wait/Release](07_sync_wait_release.md)
8. [Client Response and Commit Completion](08_client_response.md) (this chapter)

### Appendices

- [Appendix A: Symbol Index](appendix_symbol_index.md)
- [Appendix B: Glossary](appendix_glossary.md)
- [Appendix C: Configuration Parameters](appendix_config_params.md)

### Key Source Files

| File | Purpose |
|------|---------|
| `src/backend/access/transam/xlog.c` | WAL insertion, write, flush |
| `src/backend/access/transam/xloginsert.c` | Record assembly |
| `src/backend/replication/walsender.c` | Walsender process |
| `src/backend/replication/syncrep.c` | Synchronous replication |
| `src/include/replication/walsender_private.h` | Shared memory structures |

---

## Navigation

<- [Previous: Sync Wait/Release](07_sync_wait_release.md) | [Index](index.md)
