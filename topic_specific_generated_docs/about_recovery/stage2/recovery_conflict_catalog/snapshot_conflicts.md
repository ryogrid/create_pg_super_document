# Recovery Conflict Catalog: SNAPSHOT and LOGICALSLOT

The two conflict types triggered by **MVCC visibility violations**:
records being removed that an open standby snapshot still
considers visible.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `PROCSIG_RECOVERY_CONFLICT_SNAPSHOT`

* **Enum value**: `procsignal.h:45`
* **Conflict type**: a row a backend's snapshot still considers
  visible is being removed (heap pruning, btree page reuse, VM
  all-visible bit set, etc.).

### Triggering events (redo paths)

| Redo callback | Record types |
|---------------|--------------|
| `heap2_redo` | `XLOG_HEAP2_PRUNE_ON_ACCESS`, `XLOG_HEAP2_PRUNE_VACUUM_SCAN`, `XLOG_HEAP2_PRUNE_VACUUM_CLEANUP`, `XLOG_HEAP2_VISIBLE` |
| `btree_redo` | `XLOG_BTREE_DELETE`, `XLOG_BTREE_REUSE_PAGE` |
| `spg_redo` | `XLOG_SPGIST_VACUUM_REDIRECT` |
| `gist_redo` | `XLOG_GIST_PAGE_REUSE` |
| `hash_redo` | `XLOG_HASH_VACUUM_ONE_PAGE` |

### Resolver

* `ResolveRecoveryConflictWithSnapshot`
  (`src/backend/storage/ipc/standby.c:467`).
* FullXid variant: `ResolveRecoveryConflictWithSnapshotFullXid`
  (`standby.c:511`).

Both call `ResolveRecoveryConflictWithVirtualXIDs` after
collecting the conflicting VXID list.

### Grace-period GUC

* `max_standby_archive_delay` (default 30s) for archive replay.
* `max_standby_streaming_delay` (default 30s) for streaming replay.
* Distinguished via `XLogReceiptTime`: streaming records have
  receipt time near now; archive records have receipt time set to
  segment-open time.

### Victim selection

`GetConflictingVirtualXIDs(snapshotConflictHorizon, dbOid)` walks
the procarray and returns every VXID whose `xmin` is older than
`snapshotConflictHorizon`. Backends with newer snapshots are
unaffected.

The horizon is **per-record**: the redo callback extracts it from
the WAL record (`xl_btree_delete.snapshotConflictHorizon`,
`xl_heap_prune.snapshotConflictHorizon`, etc.) — it represents
the oldest xid that could still need to see the about-to-be-removed
data.

### Backend response

`ProcessRecoveryConflictInterrupt(reason=SNAPSHOT)`:

* If the backend is processing a catalog tuple (which would crash
  the world if cancelled) ⇒ `ereport(FATAL)`.
* Else ⇒ `ereport(ERROR, "canceling statement due to conflict
  with recovery", DETAIL "User query might have needed to see row
  versions that must be removed.")`.

### Logging

When `log_recovery_conflict_waits=on`, after `deadlock_timeout`
(default 1s) elapsed without resolution, `LogRecoveryConflict`
emits:

```
LOG:  recovery still waiting after 1.234 ms: recovery conflict on snapshot
DETAIL:  Conflicting process: 12345.
CONTEXT:  WAL redo at <LSN> for Heap2/PRUNE_VACUUM_SCAN: latestRemovedXid 1000000
```

### Mitigation

| Side | Workaround |
|------|------------|
| Standby | `hot_standby_feedback=on` — primary defers vacuum to respect standby xmin |
| Standby | Increase `max_standby_*_delay` (or `-1` = wait forever) |
| Standby | Run only fast queries (avoid long-running snapshots) |
| Primary | Increase `vacuum_defer_cleanup_age` (deprecated; use feedback instead) |
| Primary | Use a physical replication slot — keeps WAL but doesn't defer vacuum |

### Example scenario

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

## `PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT`

* **Enum value**: `procsignal.h:46`
* **Conflict type**: a logical replication slot's
  `catalog_xmin`/`restart_lsn` would be invalidated by replay
  (the slot's needs are violated by what we're about to apply).

### Triggering events

* Any record that advances the standby's `latestRemovedXid` past
  the slot's `catalog_xmin`.
* Drop-database records that target a slot's database.

### Resolver

The conflict is delivered to the **logical decoding consumer** that
holds the slot, not to the slot's own LSN advance path. The
mechanism is inline in `slot.c`:

* `ReplicationSlotsDropDBSlots(dbid)` for the drop-database case.
* `InvalidatePossiblyObsoleteSlot(slot, ...)` for the catalog_xmin
  case.

### Grace-period GUC

* `max_slot_wal_keep_size` — primary-side cap on how much WAL is
  kept for a stuck slot.
* `max_standby_archive_delay` / `max_standby_streaming_delay` also
  apply through the consumer's wait paths.

### Victim selection

The active backend that owns the replication slot.

### Backend response

`ereport(ERROR)` for the logical decoding consumer; the slot is
**invalidated** (cannot be used until recreated).

### Logging

```
LOG:  invalidating slot "<slotname>" because its catalog_xmin <X> is older than required by replication
```

### Mitigation

* Increase `max_slot_wal_keep_size` so the primary keeps more WAL.
* Increase the slot's `catalog_xmin` budget (set
  `hot_standby_feedback=on` if the slot is on a standby).
* Consume from the slot more aggressively.

### Example scenario

A logical replication subscriber on a standby holds a slot with
`catalog_xmin = 500`. The primary VACUUM removes catalog tuples
with `xmin < 800`. When the standby replays the catalog VACUUM,
`InvalidatePossiblyObsoleteSlot` fires; the next time the
subscriber tries to use the slot, `ereport(ERROR)` — the slot is
gone and must be recreated.

---

## Source references

* `src/include/storage/procsignal.h:45-46` —
  `PROCSIG_RECOVERY_CONFLICT_SNAPSHOT/LOGICALSLOT`
* `src/backend/storage/ipc/standby.c:467` —
  `ResolveRecoveryConflictWithSnapshot`
* `src/backend/storage/ipc/standby.c:511` —
  `ResolveRecoveryConflictWithSnapshotFullXid`
* `src/backend/storage/ipc/standby.c:359` —
  `ResolveRecoveryConflictWithVirtualXIDs`
* `src/backend/replication/slot.c` —
  `InvalidatePossiblyObsoleteSlot`,
  `ReplicationSlotsDropDBSlots`
* `src/backend/tcop/postgres.c:3074, :3232` —
  `ProcessRecoveryConflictInterrupt(s)`
