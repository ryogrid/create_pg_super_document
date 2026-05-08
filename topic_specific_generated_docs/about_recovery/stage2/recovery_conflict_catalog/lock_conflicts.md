# Recovery Conflict Catalog: LOCK

The conflict triggered when the **startup process needs an
AccessExclusiveLock** that a standby backend holds, or vice versa.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `PROCSIG_RECOVERY_CONFLICT_LOCK`

* **Enum value**: `procsignal.h:44`
* **Conflict type**: startup process's `XLOG_STANDBY_LOCK` replay
  conflicts with a standby backend's existing lock.

### Triggering event

`standby_redo` for `XLOG_STANDBY_LOCK` calls
`StandbyAcquireAccessExclusiveLock(xid, db, rel)`. Internally,
this goes through the lock manager's `LockAcquireExtended`, which
queues the startup's request behind any existing holder. If the
holder is a standby backend, `ProcSleep` returns
"sleep" — at which point `standby.c` initiates the conflict
resolution.

### Resolver

* `ResolveRecoveryConflictWithLock`
  (`src/backend/storage/ipc/standby.c:622`).
* Dispatches to `ResolveRecoveryConflictWithVirtualXIDs`.

### Grace-period GUC

* `max_standby_archive_delay` / `max_standby_streaming_delay`.
* Also: `deadlock_timeout` controls when the
  `STANDBY_LOCK_TIMEOUT` alarm logs the wait.

### Victim selection

`GetLockConflicts(locktag, lockmode)` returns every backend
holding a conflicting lock on the same locktag. Each is signaled
via `SendProcSignal(reason=PROCSIG_RECOVERY_CONFLICT_LOCK)`.

### Backend response

`ProcessRecoveryConflictInterrupt(reason=LOCK)`:

* `ereport(ERROR, "canceling statement due to conflict with
  recovery", DETAIL "User was holding a relation lock for too
  long.")`.

The transaction is aborted; the lock is released; the startup
process can now acquire its lock and continue replaying.

### Logging

`STANDBY_LOCK_TIMEOUT` alarm callback emits at the deadlock_timeout
mark:

```
LOG:  recovery still waiting for AccessExclusiveLock on relation <rel> after 1234.567 ms
DETAIL:  Conflicting processes: 12345, 12346.
```

When `log_recovery_conflict_waits=on`,
`LogRecoveryConflict(reason=LOCK)` emits.

### Mitigation

| Side | Workaround |
|------|------------|
| Standby | Avoid long-running queries on tables targeted by primary DDL |
| Standby | Increase `max_standby_*_delay` if catch-up is acceptable |
| Primary | Avoid holding AccessExclusiveLock for long (use
  `LOCK TABLE NOWAIT`, plan VACUUMs carefully, etc.) |

### Example scenario

Primary executes:
```sql
ALTER TABLE big_table ADD COLUMN x int;  -- needs AccessExclusiveLock
```

The locking is logged as `XLOG_STANDBY_LOCK { xid=12345, db=16384,
rel=20001 }`. Meanwhile, standby backend B is running:
```sql
SELECT count(*) FROM big_table;  -- holds AccessShareLock
```

When `standby_redo` runs for the lock record:
1. `StandbyAcquireAccessExclusiveLock(12345, 16384, 20001)` enqueues
   on the lock; ProcSleep starts.
2. After `deadlock_timeout` (1s), `STANDBY_LOCK_TIMEOUT` fires.
3. The alarm callback dispatches `ResolveRecoveryConflictWithLock`.
4. `GetLockConflicts(rel=20001, AccessExclusiveLock)` returns
   `[B's_vxid]`.
5. `ResolveRecoveryConflictWithVirtualXIDs(...,
   reason=PROCSIG_RECOVERY_CONFLICT_LOCK)` waits up to
   `max_standby_streaming_delay=30s`.
6. After 30s, B is canceled; lock released; startup acquires it.

---

## Source references

* `src/include/storage/procsignal.h:44` —
  `PROCSIG_RECOVERY_CONFLICT_LOCK`
* `src/backend/storage/ipc/standby.c:622` —
  `ResolveRecoveryConflictWithLock`
* `src/backend/storage/ipc/standby.c:1159` — `standby_redo`
  (XLOG_STANDBY_LOCK path)
* `src/backend/storage/ipc/standby.c` —
  `StandbyAcquireAccessExclusiveLock`
* `src/backend/postmaster/startup.c` — `StandbyLockTimeoutHandler`
  (the alarm callback)
