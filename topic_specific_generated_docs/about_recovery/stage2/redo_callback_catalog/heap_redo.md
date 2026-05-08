# Redo Callbacks: `heap_redo` and `heap2_redo`

The two heap rmgrs handle table-page modifications. Together they
account for the majority of WAL volume in most workloads. The
split between them is historical (heap2 was added later for new
record types) but has hardened into a useful boundary:
**`heap_redo` modifies tuples**, **`heap2_redo` does maintenance
operations** (pruning, freezing, visibility map, multi-insert).

[Top index for symbol-by-symbol pages](../../README.md)

---

## `heap_redo` — RM_HEAP_ID = 10

### Identity

* **rmgr id**: `RM_HEAP_ID = 10`
* **rmgr name**: `"Heap"`
* **redo function**: `heap_redo` at
  `src/backend/access/heap/heapam.c:10338`
* **header**: declared in `src/include/access/heapam_xlog.h`

### Handled records

Mask: `XLOG_HEAP_OPMASK = 0x70`.

| Info | Constant | Per-record helper |
|------|----------|-------------------|
| `0x00` | `XLOG_HEAP_INSERT` | `heap_xlog_insert` |
| `0x10` | `XLOG_HEAP_DELETE` | `heap_xlog_delete` |
| `0x20` | `XLOG_HEAP_UPDATE` | `heap_xlog_update` |
| `0x30` | `XLOG_HEAP_TRUNCATE` | (no-op in redo; logical-decoding only; smgr_redo does the actual work) |
| `0x40` | `XLOG_HEAP_HOT_UPDATE` | `heap_xlog_hot_update` |
| `0x50` | `XLOG_HEAP_CONFIRM` | `heap_xlog_confirm` (speculative-insert confirm) |
| `0x60` | `XLOG_HEAP_LOCK` | `heap_xlog_lock` |
| `0x70` | `XLOG_HEAP_INPLACE` | `heap_xlog_inplace` |

### State mutations

* Heap pages: tuple insertion/deletion/update via
  `XLogReadBufferForRedo` + `MarkBufferDirty` + `PageSetLSN`.
* `pd_lower`/`pd_upper` line pointer fields adjusted.

### Hot-standby behavior

`heap_redo` itself does **not** signal recovery conflicts. That is
`heap2_redo`'s job — heap_redo's records do not invalidate
snapshots (each modification's xid will appear in a future
COMMIT/ABORT, where snapshot bookkeeping is handled).

### Idempotency / LSN-skip

* All heap_redo paths go through `XLogReadBufferForRedo` and
  obey the `BLK_DONE` skip when `page->pd_lsn >= record_lsn`.
* The bug-prone case is `HEAP_LOCK` (it modifies the tuple's
  xmax/infomask without changing visibility) — still LSN-skipped.

### Crash safety

After replay, the heap page contents match what the primary
intended. Buffer dirty + page LSN ensures the buffer manager will
later flush, advancing minRecoveryPoint.

### Example: `XLOG_HEAP_INSERT`

```
xl_heap_insert { OffsetNumber offnum; uint8 flags; }
+ tuple data on page block 0
```

`heap_xlog_insert` body:

1. `XLogReadBufferForRedo(record, 0, &buf)`:
   * If `BLK_RESTORED` (FPI was carried) — page already has the
     tuple; nothing to do.
   * If `BLK_NEEDS_REDO` — go to step 2.
   * If `BLK_DONE` / `BLK_NOTFOUND` — skip.
2. Get xlog data via `XLogRecGetBlockData` — gives raw tuple
   header + data.
3. Place tuple at `offnum` using `PageAddItem`.
4. `PageSetLSN(page, record->EndRecPtr); MarkBufferDirty(buf);`
5. `UnlockReleaseBuffer(buf)`.

---

## `heap2_redo` — RM_HEAP2_ID = 9

### Identity

* **rmgr id**: `RM_HEAP2_ID = 9`
* **rmgr name**: `"Heap2"`
* **redo function**: `heap2_redo` at
  `src/backend/access/heap/heapam.c:10384`
* **header**: declared in `src/include/access/heapam_xlog.h`

### Handled records

| Info | Constant | Per-record helper |
|------|----------|-------------------|
| `0x10` | `XLOG_HEAP2_PRUNE_ON_ACCESS` | `heap_xlog_prune_freeze` (opportunistic prune from a SELECT) |
| `0x20` | `XLOG_HEAP2_PRUNE_VACUUM_SCAN` | `heap_xlog_prune_freeze` (vacuum scan phase) |
| `0x30` | `XLOG_HEAP2_PRUNE_VACUUM_CLEANUP` | `heap_xlog_prune_freeze` (vacuum cleanup phase) |
| `0x40` | `XLOG_HEAP2_VISIBLE` | `heap_xlog_visible` (set VM all-visible bit) |
| `0x50` | `XLOG_HEAP2_MULTI_INSERT` | `heap_xlog_multi_insert` (COPY/INSERT bulk) |
| `0x60` | `XLOG_HEAP2_LOCK_UPDATED` | `heap_xlog_lock_updated` (subtle locking case) |
| `0x70` | `XLOG_HEAP2_NEW_CID` | logical-decoding only; no-op in redo |
| `0x80` | `XLOG_HEAP2_REWRITE` | `heap_xlog_logical_rewrite` (CLUSTER/VACUUM FULL) |

### State mutations

* Heap pages (data and Visibility Map fork).
* KnownAssignedXids — indirectly via
  `ResolveRecoveryConflictWithSnapshot`.

### Hot-standby behavior

PRUNE_* and VISIBLE both call
**`ResolveRecoveryConflictWithSnapshot(snapshotConflictHorizon)`**
before applying changes:

* PRUNE_*: any tuple version that was visible to a snapshot with
  `xmin < snapshotConflictHorizon` may have been physically
  removed. Backends with such snapshots are signaled and possibly
  cancelled.
* VISIBLE: setting the VM all-visible bit indicates no in-doubt
  tuples on the page. Backends snapshotting old data must be
  cleared.

The conflict goes through
`ResolveRecoveryConflictWithVirtualXIDs` with
`PROCSIG_RECOVERY_CONFLICT_SNAPSHOT`, subject to
`max_standby_*_delay`.

### Idempotency / LSN-skip

* All paths go through `XLogReadBufferForRedo`.
* PRUNE_* records are LSN-checked. Replaying a prune that's
  already on-disk is a no-op.

### Crash safety

After replay:

* Pruned tuples are gone.
* VM bits are correctly set.
* Multi-insert: all tuples are placed on their target pages.
* Logical rewrite: pages are physically rewritten (CLUSTER /
  VACUUM FULL).

### Example: `XLOG_HEAP2_PRUNE_VACUUM_SCAN`

`heap_xlog_prune_freeze` body:

1. Extract `snapshotConflictHorizon` from the record.
2. **`ResolveRecoveryConflictWithSnapshot(snapshotConflictHorizon, rnode)`** —
   may signal & wait for backends.
3. `XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, false, &buf)`.
4. If `BLK_NEEDS_REDO`: walk the redirect/dead/unused arrays from
   the record, apply `PageRepairFragmentation`-equivalent surgery
   to the page.
5. Update VM bit if requested.
6. `PageSetLSN; MarkBufferDirty; UnlockReleaseBuffer`.

This is the canonical example of how a single redo dispatch can
cause a **standby query to be canceled**: a long-running SELECT on
the standby holds an old snapshot; the primary VACUUMs and emits
`XLOG_HEAP2_PRUNE_VACUUM_SCAN`; the standby's heap2_redo sees the
horizon, walks procarray, finds the SELECT's vxid, signals
`PROCSIG_RECOVERY_CONFLICT_SNAPSHOT`, waits up to
`max_standby_streaming_delay`, then ERRORs the SELECT.

---

## Heap masking (for `wal_consistency_checking`)

`heap_mask` (in `heapam_xlog.c`) masks volatile fields before page
comparison: hint bits (`HEAP_XMIN_COMMITTED`, `HEAP_XMAX_COMMITTED`,
etc.), `pd_lsn`, `pd_checksum`. This is what makes
`wal_consistency_checking=on` work — the just-replayed page is
masked, the FPI is masked, and `memcmp` checks they match.

---

## Source references

* `src/backend/access/heap/heapam.c:10338` — `heap_redo`
* `src/backend/access/heap/heapam.c:10384` — `heap2_redo`
* `src/backend/access/heap/heapam_xlog.c` — `heap_xlog_*` helpers,
  `heap_mask`
* `src/backend/access/heap/visibilitymap.c` — VM update helpers
* `src/include/access/heapam_xlog.h` — `XLOG_HEAP_*`,
  `XLOG_HEAP2_*` constants and payload structs
