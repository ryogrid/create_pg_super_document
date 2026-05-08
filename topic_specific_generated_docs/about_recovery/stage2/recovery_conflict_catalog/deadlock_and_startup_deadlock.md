# Recovery Conflict Catalog: STARTUP_DEADLOCK

The conflict triggered when the **startup process and a backend
form a circular wait** that cannot be resolved by the standard
deadlock detector.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK`

* **Enum value**: `procsignal.h:48`
* **Conflict type**: circular wait between startup process and a
  backend, where:
  * The backend holds a buffer pin the startup process needs
    (`LockBufferForCleanup`).
  * The startup process holds a virtual lock the backend is
    waiting for (`StandbyAcquireAccessExclusiveLock`).

### Why this can't be resolved by the regular deadlock detector

The standard PostgreSQL deadlock detector only sees lock-manager
locks. A buffer pin is **not** a lock-manager object — the
detector has no edge to find. So the cycle is invisible to
`DeadLockCheck`, and we'd hang indefinitely.

### Triggering event

Detected by `CheckRecoveryConflictDeadlock` when
`STANDBY_DEADLOCK_TIMEOUT` fires inside
`ResolveRecoveryConflictWithBufferPin`. Code path:

1. Startup tries `LockBufferForCleanup` — buffer pinned by
   backend B.
2. `STANDBY_DEADLOCK_TIMEOUT` (`= deadlock_timeout`) starts.
3. Timer fires ⇒ `CheckRecoveryConflictDeadlock`
   (`standby.c:921`):
   * Walk PGPROCs, look for any backend B such that:
     * B holds a pin on a buffer the startup needs.
     * B is waiting on a lock-manager object held by the startup
       (i.e., a virtual lock from `XLOG_STANDBY_LOCK`).
   * If found ⇒ deadlock.

### Resolver

`CheckRecoveryConflictDeadlock` directly signals
`PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK` to the candidate
backend.

### Grace-period GUC

* `deadlock_timeout` (default 1s) — when the cycle check fires.
* `max_standby_*_delay` controls the *outer* bufferpin wait, but
  the deadlock signal cancels the backend before that timeout
  expires.

### Victim selection

The single backend identified as the deadlock victim by the cycle
walk.

### Backend response

`ProcessRecoveryConflictInterrupt(reason=STARTUP_DEADLOCK)` ⇒
`ereport(FATAL)`.

**Why FATAL not ERROR**: a non-transactional buffer pin can only
be released by backend exit. ERROR would unwind the transaction
but the cursor's pin would still be held in the rebuilt
transaction-less state. FATAL guarantees the pin is released by
process exit.

### Logging

```
FATAL:  terminating connection due to conflict with recovery
DETAIL:  User transaction caused buffer deadlock with recovery.
HINT:   In a moment you should be able to reconnect to the database and repeat your command.
```

### Mitigation

* Avoid combinations of long-held cursors AND DDL on a primary
  that has standbys.
* Reduce cursor pin lifetime (use `WITH HOLD` only when truly
  needed).

### Example scenario

```
Time 0:  primary takes AccessExclusiveLock on table T (will commit later)
Time 1:  XLOG_STANDBY_LOCK record sent
Time 2:  standby_redo runs StandbyAcquireAccessExclusiveLock — startup
         now holds virtual exclusive lock on T
Time 3:  standby backend B opens cursor on table U, pins page P
Time 4:  primary VACUUMs U, emits XLOG_HEAP2_PRUNE_VACUUM_SCAN on P
Time 5:  on standby, heap2_redo runs LockBufferForCleanup on P —
         blocked because B holds a pin
Time 6:  B's next operation needs a lock on T (e.g., SELECT FROM T) —
         blocked behind startup's virtual lock
Time 7:  cycle: startup waits for B (buffer pin), B waits for startup
         (virtual lock)
Time 8:  STANDBY_DEADLOCK_TIMEOUT fires (1s default)
Time 9:  CheckRecoveryConflictDeadlock detects the cycle
Time 10: signal PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK to B
Time 11: B's next CFI -> ereport(FATAL); process exit; pin released
Time 12: startup's LockBufferForCleanup succeeds; redo continues
```

---

## Source references

* `src/include/storage/procsignal.h:48` —
  `PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK`
* `src/backend/storage/ipc/standby.c:921` —
  `CheckRecoveryConflictDeadlock`
* `src/backend/storage/ipc/standby.c:792` —
  `ResolveRecoveryConflictWithBufferPin` (caller)
* `src/backend/postmaster/startup.c` — `StandbyDeadLockHandler`
  (registered in `StartupProcessMain`)
