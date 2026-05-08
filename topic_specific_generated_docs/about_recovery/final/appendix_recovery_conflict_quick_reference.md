# Appendix E — Recovery Conflict Quick Reference

[← Redo Callback Quick Reference](appendix_redo_callback_quick_reference.md) | [index](index.md) | [next: Recovery Target Quick Reference →](appendix_recovery_target_quick_reference.md)

---

One row per `PROCSIG_RECOVERY_CONFLICT_*` enum value. For per-conflict
detail, see [18_recovery_conflict_catalog.md](18_recovery_conflict_catalog.md).
For the dispatch architecture, see
[10_hot_standby_and_recovery_conflicts.md](10_hot_standby_and_recovery_conflicts.md).

| Conflict | enum line | Trigger event | Resolver | Grace-period GUC | Backend response |
|----------|----------:|---------------|----------|-------------------|-------------------|
| `_DATABASE` | 42 | `dbase_redo XLOG_DBASE_DROP` | `ResolveRecoveryConflictWithDatabase` (`standby.c:568`) | None — no waiting | `proc_exit(1)` |
| `_TABLESPACE` | 43 | `tblspc_redo XLOG_TBLSPC_DROP` | `ResolveRecoveryConflictWithTablespace` (`standby.c:538`) → `ResolveRecoveryConflictWithVirtualXIDs` | `max_standby_archive_delay` / `max_standby_streaming_delay` | `ERROR` |
| `_LOCK` | 44 | `standby_redo XLOG_STANDBY_LOCK` → `StandbyAcquireAccessExclusiveLock` (when ProcSleep waits) | `ResolveRecoveryConflictWithLock` (`standby.c:622`) → `ResolveRecoveryConflictWithVirtualXIDs` | `max_standby_*_delay` (also `deadlock_timeout` for log emission) | `ERROR` |
| `_SNAPSHOT` | 45 | `heap2_redo XLOG_HEAP2_PRUNE_*` / `_VISIBLE`; `btree_redo XLOG_BTREE_DELETE` / `_REUSE_PAGE`; `spg_redo VACUUM_REDIRECT`; `gist_redo PAGE_REUSE`; `hash_redo VACUUM_ONE_PAGE` | `ResolveRecoveryConflictWithSnapshot` (`standby.c:467`) (FullXid variant: `:511`) → `ResolveRecoveryConflictWithVirtualXIDs` | `max_standby_*_delay` | `ERROR` (or `FATAL` if processing catalog tuples) |
| `_LOGICALSLOT` | 46 | Any record advancing standby's `latestRemovedXid` past slot's `catalog_xmin`, or DB drop targeting slot's database | `InvalidatePossiblyObsoleteSlot` (`slot.c`) / `ReplicationSlotsDropDBSlots` | `max_slot_wal_keep_size` (also `max_standby_*_delay` via consumer waits) | `ERROR` for slot consumer; slot **invalidated** |
| `_BUFFERPIN` | 47 | Any redo callback calling `LockBufferForCleanup` while a backend has the buffer pinned (typically `heap2_redo VISIBLE`, `_PRUNE_*`; `btree_redo`/`hash_redo` VACUUM-class) | `ResolveRecoveryConflictWithBufferPin` (`standby.c:792`) → `SendRecoveryConflictWithBufferPin` (broadcast) | `max_standby_*_delay` (used as `STANDBY_TIMEOUT` seed) | `ERROR`; or **release pin if idle** (special path) |
| `_STARTUP_DEADLOCK` | 48 | `CheckRecoveryConflictDeadlock` (`standby.c:921`) detects circular wait between Startup and a backend (Startup holds vlock, backend holds buffer pin) | `CheckRecoveryConflictDeadlock` directly signals victim | `deadlock_timeout` triggers detection | `FATAL` (must exit; pin can only be released by process exit) |

## Common subroutines

* **`ResolveRecoveryConflictWithVirtualXIDs`** (`standby.c:359`): the
  generic "signal then wait then cancel" loop. Used by Snapshot,
  Tablespace, Lock resolvers.
* **`WaitExceedsMaxStandbyDelay`** (`standby.c`): picks
  `max_standby_streaming_delay` vs `max_standby_archive_delay`
  based on `XLogReceiptTime`.
* **`GetConflictingVirtualXIDs`** (`procarray.c`): walks the
  procarray for VXIDs that conflict with a given horizon.
* **`SendProcSignal`** (`procsignal.c`): generic signal sender that
  delivers `PROCSIG_RECOVERY_CONFLICT_*` to a specific PID.

## Backend-side dispatch

```
SIGUSR1 → procsignal_sigusr1_handler
       → HandleRecoveryConflictInterrupt(reason)  /* postgres.c:3062 */
                ↓ sets RecoveryConflictPending = true
                  sets RecoveryConflictPendingReasons[reason] = true
                  sets InterruptPending

next CHECK_FOR_INTERRUPTS()
       → ProcessInterrupts
       → ProcessRecoveryConflictInterrupts          /* postgres.c:3232 */
       → ProcessRecoveryConflictInterrupt(reason)   /* postgres.c:3074 */
                ↓ dispatches per reason:
                   DATABASE         → proc_exit(1)
                   TABLESPACE/LOCK/
                       SNAPSHOT     → ereport(ERROR)
                   BUFFERPIN        → if idle: release pin
                                       else: ereport(ERROR)
                   LOGICALSLOT      → ereport(ERROR) for slot consumer
                   STARTUP_DEADLOCK → ereport(FATAL)
```

## Logging

When `log_recovery_conflict_waits = on`, after `deadlock_timeout`
elapses without the conflict being resolved, `LogRecoveryConflict`
(`standby.c:282`) emits a LOG entry like:

```
LOG:  recovery still waiting after 1234.567 ms: recovery conflict on snapshot
DETAIL:  Conflicting process: 12345.
CONTEXT:  WAL redo at 0/A0001234 for Heap2/PRUNE_VACUUM_SCAN: latestRemovedXid 1000000
```
