# Redo Callback: `standby_redo`

The single most important rmgr for hot standby. It replays records
that the primary emits explicitly to keep the standby's view of
running transactions, AccessExclusiveLocks, and shared invalidation
messages consistent.

[Top index for symbol-by-symbol pages](../../README.md)

---

## Identity

* **rmgr id**: `RM_STANDBY_ID = 8`
* **rmgr name**: `"Standby"`
* **redo function**: `standby_redo` at
  `src/backend/storage/ipc/standby.c:1159`
* **header**: declared in `src/include/storage/standby.h`

## Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_STANDBY_LOCK` | Per-record list of AccessExclusiveLocks |
| `0x10` | `XLOG_RUNNING_XACTS` | Snapshot of primary's procarray |
| `0x20` | `XLOG_INVALIDATIONS` | Standalone-inval message broadcast |

### Payload structs (`src/include/storage/standby.h`)

```c
typedef struct xl_standby_lock
{
    TransactionId   xid;        /* primary xid that owns the lock */
    Oid             dbOid;
    Oid             relOid;
} xl_standby_lock;

typedef struct xl_standby_locks
{
    int             nlocks;
    xl_standby_lock locks[FLEXIBLE_ARRAY_MEMBER];
} xl_standby_locks;

typedef struct xl_running_xacts
{
    int             xcnt;
    int             subxcnt;
    bool            subxid_overflow;
    TransactionId   nextXid;
    TransactionId   oldestRunningXid;
    TransactionId   latestCompletedXid;
    TransactionId   xids[FLEXIBLE_ARRAY_MEMBER];
} xl_running_xacts;

typedef struct xl_invalidations
{
    Oid             dbId;
    Oid             tsId;
    bool            relcacheInitFileInval;
    int             nmsgs;
    SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} xl_invalidations;
```

## State mutations

| Target | Action | Triggered by |
|--------|--------|--------------|
| Lock manager | `StandbyAcquireAccessExclusiveLock` (per-lock) | `XLOG_STANDBY_LOCK` |
| KnownAssignedXids | Reset and repopulate | `XLOG_RUNNING_XACTS` |
| `standbyState` | INITIALIZED → SNAPSHOT_READY (or PENDING) | `XLOG_RUNNING_XACTS` |
| `pg_subtrans` | Subxid → parent mappings | `XLOG_RUNNING_XACTS` |
| sinval queue | `ProcessCommittedInvalidationMessages` | `XLOG_INVALIDATIONS` |

## Hot-standby behavior

This **is** the rmgr for hot-standby setup. It is the source of:

* All virtual locks the standby holds on behalf of primary
  transactions.
* The `KnownAssignedXids` snapshot used by every `GetSnapshotData`
  on the standby.
* Catalog-invalidation messages from primary-side
  `StartTransactionCommand`s (so standby backends notice DDL).

It also gates the `STANDBY_INITIALIZED → SNAPSHOT_PENDING →
SNAPSHOT_READY` state machine. Until SNAPSHOT_READY, no
hot-standby query can run.

## Conflict generation

`XLOG_STANDBY_LOCK` may emit `PROCSIG_RECOVERY_CONFLICT_LOCK` via
`StandbyAcquireAccessExclusiveLock` when `ProcSleep` decides the
backend already holds a conflicting lock. See
[recovery_conflict_catalog/lock_conflicts.md](../recovery_conflict_catalog/lock_conflicts.md).

## Idempotency / LSN-skip

* `XLOG_STANDBY_LOCK`: idempotent — re-acquiring the same virtual
  lock is a no-op.
* `XLOG_RUNNING_XACTS`: idempotent — repopulating
  KnownAssignedXids from the same set yields the same state.
* `XLOG_INVALIDATIONS`: idempotent — re-broadcasting inval
  messages is harmless (consumers handle duplicates).
* No data-page writes; no page-LSN check.

## Crash safety

`standby_redo` does not produce any new on-disk durability
guarantees — its mutations are all in shared memory. Crash safety
is established by:

* `XLOG_STANDBY_LOCK` — locks are auto-released at recovery exit
  by `StandbyReleaseAllLocks`, OR re-replayed from WAL on the next
  recovery.
* `XLOG_RUNNING_XACTS` — the standby's KnownAssignedXids is
  re-built on every restart by replaying these records.

## Example records

### Example 1: `XLOG_STANDBY_LOCK`

```
xl_standby_locks { nlocks=1, locks=[{xid=12345, dbOid=16384, relOid=20001}] }
```

`standby_redo` calls `StandbyAcquireAccessExclusiveLock(12345,
16384, 20001)`. Standby backends now block waiting for that virtual
lock to be released.

### Example 2: `XLOG_RUNNING_XACTS`

```
xl_running_xacts { xcnt=2, subxcnt=0, subxid_overflow=false,
                   nextXid=12350, oldestRunningXid=12340,
                   latestCompletedXid=12339,
                   xids=[12345, 12346] }
```

`standby_redo` calls `ProcArrayApplyRecoveryInfo(running)`:

1. Reset `KnownAssignedXids` ring.
2. Add 12345, 12346.
3. Update `latestCompletedXid = 12339`,
   `nextXid = max(nextXid, 12350)`.
4. `standbyState = STANDBY_SNAPSHOT_READY`.
5. Broadcast that hot standby is now consistent
   (`PMSIGNAL_BEGIN_HOT_STANDBY`).

### Example 3: `XLOG_INVALIDATIONS`

```
xl_invalidations { dbId=16384, tsId=1663, nmsgs=3, msgs=[...] }
```

`standby_redo` calls
`ProcessCommittedInvalidationMessages(msgs, 3)` — broadcasts to
shared invalidation queue. Standby backends will notice the
catalog change at next CFI.

This record is emitted by the primary in
`StartTransactionCommand` when the in-flight transaction has
already broadcast invalidation messages but has not yet committed.
The "early broadcast" feature prevents the standby from missing
inval messages on long-running primary transactions.

---

## Source references

* `src/backend/storage/ipc/standby.c:1159` — `standby_redo`
* `src/backend/storage/ipc/standby.c` — `StandbyAcquireAccessExclusiveLock`
* `src/backend/storage/ipc/standby.c` — `LogStandbySnapshot`
  (primary side)
* `src/backend/storage/ipc/procarray.c` — `ProcArrayApplyRecoveryInfo`,
  `ProcArrayApplyXidAssignment`,
  `ExpireTreeKnownAssignedTransactionIds`
* `src/include/storage/standby.h` — `xl_standby_lock`,
  `xl_running_xacts`, `xl_invalidations`
* `src/backend/storage/ipc/sinval.c` —
  `ProcessCommittedInvalidationMessages`
