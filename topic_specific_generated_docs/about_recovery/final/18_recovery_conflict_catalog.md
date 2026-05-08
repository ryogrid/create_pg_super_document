# 18 — Recovery Conflict Catalog

[← Redo Callback Catalog](17_redo_callback_catalog.md) | [index](index.md) | [next: Recovery Target Catalog →](19_recovery_target_catalog.md)

---

This chapter is a per-conflict reference for the **7
`PROCSIG_RECOVERY_CONFLICT_*` enum values** declared in
`src/include/storage/procsignal.h:42-48`. Each entry follows the
standardized template:

* **Enum value and conflict type.**
* **Triggering event** — which redo callback (or condition) emits it.
* **Resolver function** — the `Resolve*` family member in
  `src/backend/storage/ipc/standby.c`.
* **Grace period GUC** — typically `max_standby_archive_delay` /
  `max_standby_streaming_delay`.
* **Victim selection** — how `GetConflictingVirtualXIDs` /
  `CountDBBackends` etc. pick whom to signal.
* **Backend response** — `ERROR`, `FATAL`, or special-case (release
  pin).
* **Logging** — `log_recovery_conflict_waits` integration.
* **Mitigation** — operator-side workarounds.
* **Example scenario** — concrete primary→standby timeline.

For the architecture (cross-process model, `ResolveRecoveryConflictWithVirtualXIDs`
common subroutine, backend-side dispatcher), see
[10_hot_standby_and_recovery_conflicts.md](10_hot_standby_and_recovery_conflicts.md).

For a one-line-per-conflict overview, see
[appendix_recovery_conflict_quick_reference.md](appendix_recovery_conflict_quick_reference.md).

## Catalog overview

| `PROCSIG_RECOVERY_CONFLICT_*` value | enum line | Resolver | Section |
|-------------------------------------|----------:|----------|---------|
| `_DATABASE` | 42 | `ResolveRecoveryConflictWithDatabase` | [§1](#1-procsig_recovery_conflict_database) |
| `_TABLESPACE` | 43 | `ResolveRecoveryConflictWithTablespace` | [§2](#2-procsig_recovery_conflict_tablespace) |
| `_LOCK` | 44 | `ResolveRecoveryConflictWithLock` | [§3](#3-procsig_recovery_conflict_lock) |
| `_SNAPSHOT` | 45 | `ResolveRecoveryConflictWithSnapshot` | [§4](#4-procsig_recovery_conflict_snapshot) |
| `_LOGICALSLOT` | 46 | `InvalidatePossiblyObsoleteSlot` (in `slot.c`) | [§5](#5-procsig_recovery_conflict_logicalslot) |
| `_BUFFERPIN` | 47 | `ResolveRecoveryConflictWithBufferPin` | [§6](#6-procsig_recovery_conflict_bufferpin) |
| `_STARTUP_DEADLOCK` | 48 | `CheckRecoveryConflictDeadlock` | [§7](#7-procsig_recovery_conflict_startup_deadlock) |

---

## 1. `PROCSIG_RECOVERY_CONFLICT_DATABASE`

* **Enum value**: `procsignal.h:42`
* **Conflict type**: a database is being dropped while standby
  backends are connected to it.

#### Triggering event

`dbase_redo XLOG_DBASE_DROP` — the database directory is about to
be `rmtree`'d.

#### Resolver

* `ResolveRecoveryConflictWithDatabase`
  (`src/backend/storage/ipc/standby.c:568`).
* **No grace period** — DB is gone now, not later.

#### Grace-period GUC

None — no waiting at all.

#### Victim selection

`CountDBBackends(dbid)` walks the procarray. Every backend with
`MyDatabaseId == dbid` is signaled via
`CancelDBBackends(dbid, PROCSIG_RECOVERY_CONFLICT_DATABASE,
/*conflictPending=*/true)`.

#### Backend response

`ProcessRecoveryConflictInterrupt(reason=DATABASE)` ⇒
`proc_exit(1)`. The backend cannot recover — its database is
gone. There's no point in `ereport(ERROR)` since the next
operation would fail at the filesystem layer anyway.

#### Logging

```
FATAL:  terminating connection due to conflict with recovery
DETAIL:  User was connected to a database that must be dropped.
```

#### Mitigation

Don't drop databases on the primary while standby backends are
connected. (This is a transient operational issue — the standby
catches up, the database is gone, new connections succeed against
remaining databases.)

#### Example scenario

Primary executes `DROP DATABASE devdb`. Two backends are
connected to devdb on the standby. When `dbase_redo` runs:

1. `ResolveRecoveryConflictWithDatabase(devdb_oid)`:
   * Walk procarray; find the two backends.
   * `SendProcSignal(reason=DATABASE)` to each.
   * No wait — return immediately.
2. `DropDatabaseBuffers(devdb_oid)`.
3. `rmtree("base/devdb_oid")`.
4. The two backends, on next CFI, `proc_exit(1)`.

---

---

## 2. `PROCSIG_RECOVERY_CONFLICT_TABLESPACE`

* **Enum value**: `procsignal.h:43`
* **Conflict type**: a tablespace is being dropped that contains
  in-use temp files (or other in-use files) belonging to standby
  backends.

#### Triggering event

`tblspc_redo XLOG_TBLSPC_DROP`.

#### Resolver

* `ResolveRecoveryConflictWithTablespace`
  (`src/backend/storage/ipc/standby.c:538`).
* Dispatches to `ResolveRecoveryConflictWithVirtualXIDs`.

#### Grace-period GUC

* `max_standby_archive_delay` / `max_standby_streaming_delay`.

#### Victim selection

`GetConflictingVirtualXIDs(InvalidTransactionId, InvalidOid)` for
backends with **temp files** in the target tablespace. The temp
namespace is per-backend; the procarray entries that reference
`temp_tablespaces` containing the target are flagged.

#### Backend response

`ProcessRecoveryConflictInterrupt(reason=TABLESPACE)` ⇒
`ereport(ERROR, "canceling statement due to conflict with
recovery", DETAIL "User was using a tablespace that must be
dropped.")`.

#### Logging

When `log_recovery_conflict_waits=on`,
`LogRecoveryConflict(reason=TABLESPACE)`.

#### Mitigation

* Coordinate tablespace drops with standby workload.
* Increase `max_standby_*_delay`.

#### Example scenario

Primary executes `DROP TABLESPACE testts`. Standby backend has a
temp table in `testts`. When `tblspc_redo` runs:

1. `ResolveRecoveryConflictWithTablespace(testts_oid)`:
   * Walk procarray for backends with temp files in testts_oid.
   * For each: `SendProcSignal(reason=TABLESPACE)`.
   * Wait up to `max_standby_streaming_delay`.
2. After timeout (or earlier release): backends are canceled.
3. `destroy_tablespace_directories(testts_oid, true)`.
4. `unlink("pg_tblspc/testts_oid")`.

---

---

## 3. `PROCSIG_RECOVERY_CONFLICT_LOCK`

* **Enum value**: `procsignal.h:44`
* **Conflict type**: startup process's `XLOG_STANDBY_LOCK` replay
  conflicts with a standby backend's existing lock.

#### Triggering event

`standby_redo` for `XLOG_STANDBY_LOCK` calls
`StandbyAcquireAccessExclusiveLock(xid, db, rel)`. Internally,
this goes through the lock manager's `LockAcquireExtended`, which
queues the startup's request behind any existing holder. If the
holder is a standby backend, `ProcSleep` returns
"sleep" — at which point `standby.c` initiates the conflict
resolution.

#### Resolver

* `ResolveRecoveryConflictWithLock`
  (`src/backend/storage/ipc/standby.c:622`).
* Dispatches to `ResolveRecoveryConflictWithVirtualXIDs`.

#### Grace-period GUC

* `max_standby_archive_delay` / `max_standby_streaming_delay`.
* Also: `deadlock_timeout` controls when the
  `STANDBY_LOCK_TIMEOUT` alarm logs the wait.

#### Victim selection

`GetLockConflicts(locktag, lockmode)` returns every backend
holding a conflicting lock on the same locktag. Each is signaled
via `SendProcSignal(reason=PROCSIG_RECOVERY_CONFLICT_LOCK)`.

#### Backend response

`ProcessRecoveryConflictInterrupt(reason=LOCK)`:

* `ereport(ERROR, "canceling statement due to conflict with
  recovery", DETAIL "User was holding a relation lock for too
  long.")`.

The transaction is aborted; the lock is released; the startup
process can now acquire its lock and continue replaying.

#### Logging

`STANDBY_LOCK_TIMEOUT` alarm callback emits at the deadlock_timeout
mark:

```
LOG:  recovery still waiting for AccessExclusiveLock on relation <rel> after 1234.567 ms
DETAIL:  Conflicting processes: 12345, 12346.
```

When `log_recovery_conflict_waits=on`,
`LogRecoveryConflict(reason=LOCK)` emits.

#### Mitigation

| Side | Workaround |
|------|------------|
| Standby | Avoid long-running queries on tables targeted by primary DDL |
| Standby | Increase `max_standby_*_delay` if catch-up is acceptable |
| Primary | Avoid holding AccessExclusiveLock for long (use
  `LOCK TABLE NOWAIT`, plan VACUUMs carefully, etc.) |

#### Example scenario

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

---

## 4. `PROCSIG_RECOVERY_CONFLICT_SNAPSHOT`

* **Enum value**: `procsignal.h:45`
* **Conflict type**: a row a backend's snapshot still considers
  visible is being removed (heap pruning, btree page reuse, VM
  all-visible bit set, etc.).

#### Triggering events (redo paths)

| Redo callback | Record types |
|---------------|--------------|
| `heap2_redo` | `XLOG_HEAP2_PRUNE_ON_ACCESS`, `XLOG_HEAP2_PRUNE_VACUUM_SCAN`, `XLOG_HEAP2_PRUNE_VACUUM_CLEANUP`, `XLOG_HEAP2_VISIBLE` |
| `btree_redo` | `XLOG_BTREE_DELETE`, `XLOG_BTREE_REUSE_PAGE` |
| `spg_redo` | `XLOG_SPGIST_VACUUM_REDIRECT` |
| `gist_redo` | `XLOG_GIST_PAGE_REUSE` |
| `hash_redo` | `XLOG_HASH_VACUUM_ONE_PAGE` |

#### Resolver

* `ResolveRecoveryConflictWithSnapshot`
  (`src/backend/storage/ipc/standby.c:467`).
* FullXid variant: `ResolveRecoveryConflictWithSnapshotFullXid`
  (`standby.c:511`).

Both call `ResolveRecoveryConflictWithVirtualXIDs` after
collecting the conflicting VXID list.

#### Grace-period GUC

* `max_standby_archive_delay` (default 30s) for archive replay.
* `max_standby_streaming_delay` (default 30s) for streaming replay.
* Distinguished via `XLogReceiptTime`: streaming records have
  receipt time near now; archive records have receipt time set to
  segment-open time.

#### Victim selection

`GetConflictingVirtualXIDs(snapshotConflictHorizon, dbOid)` walks
the procarray and returns every VXID whose `xmin` is older than
`snapshotConflictHorizon`. Backends with newer snapshots are
unaffected.

The horizon is **per-record**: the redo callback extracts it from
the WAL record (`xl_btree_delete.snapshotConflictHorizon`,
`xl_heap_prune.snapshotConflictHorizon`, etc.) — it represents
the oldest xid that could still need to see the about-to-be-removed
data.

#### Backend response

`ProcessRecoveryConflictInterrupt(reason=SNAPSHOT)`:

* If the backend is processing a catalog tuple (which would crash
  the world if cancelled) ⇒ `ereport(FATAL)`.
* Else ⇒ `ereport(ERROR, "canceling statement due to conflict
  with recovery", DETAIL "User query might have needed to see row
  versions that must be removed.")`.

#### Logging

When `log_recovery_conflict_waits=on`, after `deadlock_timeout`
(default 1s) elapsed without resolution, `LogRecoveryConflict`
emits:

```
LOG:  recovery still waiting after 1.234 ms: recovery conflict on snapshot
DETAIL:  Conflicting process: 12345.
CONTEXT:  WAL redo at <LSN> for Heap2/PRUNE_VACUUM_SCAN: latestRemovedXid 1000000
```

#### Mitigation

| Side | Workaround |
|------|------------|
| Standby | `hot_standby_feedback=on` — primary defers vacuum to respect standby xmin |
| Standby | Increase `max_standby_*_delay` (or `-1` = wait forever) |
| Standby | Run only fast queries (avoid long-running snapshots) |
| Primary | Increase `vacuum_defer_cleanup_age` (deprecated; use feedback instead) |
| Primary | Use a physical replication slot — keeps WAL but doesn't defer vacuum |

#### Example scenario

```
Time 0:  standby backend B starts SELECT * FROM big_table (snapshot xmin=500)
Time 1:  primary VACUUM big_table marks tuples xmin<800 as dead
Time 2:  primary emits XLOG_HEAP2_PRUNE_VACUUM_SCAN with horizon=800
Time 3:  walreceiver delivers record; startup heap2_redo runs
Time 4:  ResolveRecoveryConflictWithSnapshot(800) finds backend B (xmin=500<800)
Time 5:  signal PROCSIG_RECOVERY_CONFLICT_SNAPSHOT to B
Time 6:  startup waits up to max_standby_streaming_delay=30s
Time 7:  if B doesn't finish: CancelVirtualTransaction(vxid_B, SNAPSHOT)
Time 8:  B's next CFI: ereport(ERROR) — query cancelled
Time 9:  startup proceeds with redo
```

---

---

## 5. `PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT`

* **Enum value**: `procsignal.h:46`
* **Conflict type**: a logical replication slot's
  `catalog_xmin`/`restart_lsn` would be invalidated by replay
  (the slot's needs are violated by what we're about to apply).

#### Triggering events

* Any record that advances the standby's `latestRemovedXid` past
  the slot's `catalog_xmin`.
* Drop-database records that target a slot's database.

#### Resolver

The conflict is delivered to the **logical decoding consumer** that
holds the slot, not to the slot's own LSN advance path. The
mechanism is inline in `slot.c`:

* `ReplicationSlotsDropDBSlots(dbid)` for the drop-database case.
* `InvalidatePossiblyObsoleteSlot(slot, ...)` for the catalog_xmin
  case.

#### Grace-period GUC

* `max_slot_wal_keep_size` — primary-side cap on how much WAL is
  kept for a stuck slot.
* `max_standby_archive_delay` / `max_standby_streaming_delay` also
  apply through the consumer's wait paths.

#### Victim selection

The active backend that owns the replication slot.

#### Backend response

`ereport(ERROR)` for the logical decoding consumer; the slot is
**invalidated** (cannot be used until recreated).

#### Logging

```
LOG:  invalidating slot "<slotname>" because its catalog_xmin <X> is older than required by replication
```

#### Mitigation

* Increase `max_slot_wal_keep_size` so the primary keeps more WAL.
* Increase the slot's `catalog_xmin` budget (set
  `hot_standby_feedback=on` if the slot is on a standby).
* Consume from the slot more aggressively.

#### Example scenario

A logical replication subscriber on a standby holds a slot with
`catalog_xmin = 500`. The primary VACUUM removes catalog tuples
with `xmin < 800`. When the standby replays the catalog VACUUM,
`InvalidatePossiblyObsoleteSlot` fires; the next time the
subscriber tries to use the slot, `ereport(ERROR)` — the slot is
gone and must be recreated.

---

---

## 6. `PROCSIG_RECOVERY_CONFLICT_BUFFERPIN`

* **Enum value**: `procsignal.h:47`
* **Conflict type**: startup process needs to acquire
  `LockBufferForCleanup` (an exclusive pin) on a shared buffer
  another backend has pinned.

#### Triggering event

Any redo callback that calls `LockBufferForCleanup` while the
buffer is pinned by another backend. In practice:

* `heap2_redo XLOG_HEAP2_VISIBLE` — setting the VM all-visible bit
  requires a cleanup lock on the heap page.
* `heap2_redo XLOG_HEAP2_PRUNE_*` — pruning needs cleanup lock.
* `btree_redo`, `hash_redo` VACUUM-class records.

#### Resolver

* `ResolveRecoveryConflictWithBufferPin`
  (`src/backend/storage/ipc/standby.c:792`).
* Different from the others: instead of building a VXID list and
  calling `ResolveRecoveryConflictWithVirtualXIDs`, it sets a
  `STANDBY_TIMEOUT` alarm, then signals **every** active backend
  via `SendRecoveryConflictWithBufferPin`. Backends that don't
  hold the relevant pin ignore the signal (filtered via
  `RecoveryConflictPendingReasons[]`).

#### Grace-period GUC

* `max_standby_archive_delay` / `max_standby_streaming_delay`
  — used as the `STANDBY_TIMEOUT` seed.

#### Victim selection

The signal is broadcast to all backends; only those actually
holding pins on the targeted buffer set
`RecoveryConflictPending`. The others simply observe
`RecoveryConflictPendingReasons[BUFFERPIN] == false` (no pending
work for them) at next CFI.

#### Backend response

`ProcessRecoveryConflictInterrupt(reason=BUFFERPIN)`:

* If the backend is **idle** (no statement in progress) AND is the
  one blocking startup ⇒ release the buffer pin without canceling
  (special path — pin can be released without aborting the
  transaction).
* Else ⇒ `ereport(ERROR, "canceling statement due to conflict
  with recovery")`.

The "release pin if idle" path is a real performance optimization:
many idle psql sessions sit on buffers via cursors; canceling them
would be heavy-handed when the pin can simply be dropped.

#### Logging

When `log_recovery_conflict_waits=on`,
`LogRecoveryConflict(reason=BUFFERPIN)`.

#### Mitigation

* Avoid long-held cursors on a standby (their pins can block
  vacuum-related redo).
* Increase `max_standby_*_delay`.

#### Example scenario

A backend on the standby is reading a large table via a holdable
cursor (which keeps a buffer pin between fetches). The primary
runs VACUUM and emits `XLOG_HEAP2_VISIBLE` to set the VM bit on
that page. When `heap2_redo` runs:

1. `XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL,
   /*get_cleanup_lock=*/true, &buf)` — needs a *cleanup* lock.
2. `LockBufferForCleanup` returns: buffer is pinned by another
   backend.
3. Set `STANDBY_TIMEOUT = max_standby_streaming_delay`.
4. `SendRecoveryConflictWithBufferPin` — broadcast signal.
5. Wait on a sleep loop polling `LockBufferForCleanup`.
6. If the cursor backend is idle: it releases its pin via the
   special-case path, startup acquires the cleanup lock, applies
   the VM update.
7. Else, after STANDBY_TIMEOUT: backend cancelled, pin released,
   startup proceeds.

---

---

## 7. `PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK`

* **Enum value**: `procsignal.h:48`
* **Conflict type**: circular wait between startup process and a
  backend, where:
  * The backend holds a buffer pin the startup process needs
    (`LockBufferForCleanup`).
  * The startup process holds a virtual lock the backend is
    waiting for (`StandbyAcquireAccessExclusiveLock`).

#### Why this can't be resolved by the regular deadlock detector

The standard PostgreSQL deadlock detector only sees lock-manager
locks. A buffer pin is **not** a lock-manager object — the
detector has no edge to find. So the cycle is invisible to
`DeadLockCheck`, and we'd hang indefinitely.

#### Triggering event

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

#### Resolver

`CheckRecoveryConflictDeadlock` directly signals
`PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK` to the candidate
backend.

#### Grace-period GUC

* `deadlock_timeout` (default 1s) — when the cycle check fires.
* `max_standby_*_delay` controls the *outer* bufferpin wait, but
  the deadlock signal cancels the backend before that timeout
  expires.

#### Victim selection

The single backend identified as the deadlock victim by the cycle
walk.

#### Backend response

`ProcessRecoveryConflictInterrupt(reason=STARTUP_DEADLOCK)` ⇒
`ereport(FATAL)`.

**Why FATAL not ERROR**: a non-transactional buffer pin can only
be released by backend exit. ERROR would unwind the transaction
but the cursor's pin would still be held in the rebuilt
transaction-less state. FATAL guarantees the pin is released by
process exit.

#### Logging

```
FATAL:  terminating connection due to conflict with recovery
DETAIL:  User transaction caused buffer deadlock with recovery.
HINT:   In a moment you should be able to reconnect to the database and repeat your command.
```

#### Mitigation

* Avoid combinations of long-held cursors AND DDL on a primary
  that has standbys.
* Reduce cursor pin lifetime (use `WITH HOLD` only when truly
  needed).

#### Example scenario

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

---


## Cross-references

* For the architecture (`ResolveRecoveryConflictWithVirtualXIDs`, `HandleRecoveryConflictInterrupt`, `ProcessRecoveryConflictInterrupt`): [10_hot_standby_and_recovery_conflicts.md](10_hot_standby_and_recovery_conflicts.md).
* For the redo callbacks that trigger these conflicts: [17_redo_callback_catalog.md](17_redo_callback_catalog.md).
* For the `xl_standby_*` payload structs and `VirtualTransactionId`: [appendix_data_structures.md](appendix_data_structures.md).
* For one-line-per-conflict overview: [appendix_recovery_conflict_quick_reference.md](appendix_recovery_conflict_quick_reference.md).
* For the deep-dive on `KnownAssignedXids` ring mechanics: [20_deep_dives.md](20_deep_dives.md).