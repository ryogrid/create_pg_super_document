# Redo Callback: `btree_redo`

The B-tree index rmgr. The most complex single-rmgr redo callback
because of the **incomplete-split tracker** machinery: a btree
split is a multi-record operation (split logged, then later parent
update logged), and recovery must handle the case where it crashes
between the two records.

[Top index for symbol-by-symbol pages](../../README.md)

---

## Identity

* **rmgr id**: `RM_BTREE_ID = 11`
* **rmgr name**: `"Btree"`
* **redo function**: `btree_redo` at
  `src/backend/access/nbtree/nbtxlog.c:1014`
* **rm_startup**: `btree_xlog_startup` (init incomplete-split tracker)
* **rm_cleanup**: `btree_xlog_cleanup` (finish leftover splits)
* **header**: declared in `src/include/access/nbtxlog.h`

## Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_BTREE_INSERT_LEAF` | Insert into leaf |
| `0x10` | `XLOG_BTREE_INSERT_UPPER` | Insert into internal page |
| `0x20` | `XLOG_BTREE_INSERT_META` | Insert touches meta page |
| `0x30` | `XLOG_BTREE_SPLIT_L` | Split left page kept |
| `0x40` | `XLOG_BTREE_SPLIT_R` | Split right page kept |
| `0x50` | `XLOG_BTREE_INSERT_POST` | Posting-list insert |
| `0x60` | `XLOG_BTREE_DEDUP` | Deduplication |
| `0x70` | `XLOG_BTREE_DELETE` | Delete from leaf — **emits snapshot conflict** |
| `0x80` | `XLOG_BTREE_UNLINK_PAGE` | Unlink (after VACUUM marked half-dead) |
| `0x90` | `XLOG_BTREE_UNLINK_PAGE_META` | Unlink updates meta |
| `0xA0` | `XLOG_BTREE_NEWROOT` | New root after split |
| `0xB0` | `XLOG_BTREE_MARK_PAGE_HALFDEAD` | Page becoming dead |
| `0xC0` | `XLOG_BTREE_VACUUM` | Bulk delete during VACUUM |
| `0xD0` | `XLOG_BTREE_REUSE_PAGE` | Reuse page — **emits snapshot conflict horizon** |

Payload structs (in `nbtxlog.h`):

* `xl_btree_insert`
* `xl_btree_split` / `xl_btree_split_alt`
* `xl_btree_dedup`
* `xl_btree_delete`
* `xl_btree_reuse_page`
* `xl_btree_unlink_page`
* `xl_btree_metadata`
* `xl_btree_newroot`

## State mutations

* B-tree index pages.
* A separate `incomplete_split` hash table maintained across the
  redo loop (initialized by `rm_startup`, drained by
  `rm_cleanup`).

## Incomplete-split tracker

A B-tree split logs:

1. `XLOG_BTREE_SPLIT_L` (or `_R`): both leaf pages have the new
   split layout, but the parent has not yet been updated.
2. `XLOG_BTREE_INSERT_UPPER` later: the parent page learns about
   the new right sibling.

If recovery sees only step 1 (because the cluster crashed before
step 2 was emitted), the tree is *temporarily inconsistent* — the
right sibling exists but no parent points to it.

`btree_xlog_startup` allocates a hash table keyed by `(rel,
left-block)`. `btree_redo` for SPLIT inserts an entry; for
INSERT_UPPER removes it. `btree_xlog_cleanup` walks any leftover
entries and finishes the parent update by calling
`_bt_finish_split` directly on the index.

## Hot-standby behavior

* `XLOG_BTREE_DELETE`: emits
  `ResolveRecoveryConflictWithSnapshot(snapshotConflictHorizon)`.
* `XLOG_BTREE_REUSE_PAGE`: emits
  `ResolveRecoveryConflictWithSnapshot(latestRemovedFullXid)`.

These conflicts are needed because index tuples being removed
might still be visible to a backend's snapshot — the heap-level
conflict (`XLOG_HEAP2_PRUNE_*`) is not always sufficient because
the index can be pruned independently of the heap.

## Idempotency / LSN-skip

* All page modifications go through `XLogReadBufferForRedo` with
  LSN-skip.
* The incomplete-split tracker is *not* idempotent across
  recovery runs — but `rm_startup` initializes it from scratch
  each time, so re-replaying SPLIT records gives the right
  end state.

## Crash safety

After `rm_cleanup` runs, the B-tree is consistent: every leaf
split has a corresponding parent entry. Index scans see the
correct structure.

## Example: `XLOG_BTREE_DELETE`

```c
xl_btree_delete {
    TransactionId snapshotConflictHorizon;
    uint16       ndeleted;
    uint16       nupdated;
    /* Followed by deleted offset numbers + updated offset numbers. */
}
```

`btree_redo` for DELETE:

1. Extract `snapshotConflictHorizon`.
2. **`ResolveRecoveryConflictWithSnapshot(horizon, rnode)`** —
   wait/cancel backends with old snapshots.
3. `XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, false, &buf)`.
4. If `BLK_NEEDS_REDO`:
   * Read deleted offsets array from xlog data.
   * For each deleted offset: `PageIndexTupleDelete`.
   * For each updated offset (posting-list update): apply the new
     posting list bytes.
   * `PageSetLSN; MarkBufferDirty`.
5. Release buffer.

---

## Source references

* `src/backend/access/nbtree/nbtxlog.c:1014` — `btree_redo`
* `src/backend/access/nbtree/nbtxlog.c` — `btree_xlog_startup`,
  `btree_xlog_cleanup`, `btree_xlog_delete`,
  `btree_xlog_reuse_page`, `_bt_finish_split`
* `src/include/access/nbtxlog.h` — `XLOG_BTREE_*` constants and
  payload structs
