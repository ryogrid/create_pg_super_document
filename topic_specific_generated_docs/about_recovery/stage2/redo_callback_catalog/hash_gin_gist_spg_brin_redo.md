# Redo Callbacks: Other Index AMs

`hash_redo`, `gin_redo`, `gist_redo`, `spg_redo`, `brin_redo` —
the index access methods other than B-tree. Each has its own
WAL record set; all five follow the standard
`XLogReadBufferForRedo`-based pattern.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `hash_redo` — RM_HASH_ID = 12

* **redo function**: `hash_redo` at
  `src/backend/access/hash/hash_xlog.c:1067`
* **header**: `src/include/access/hash_xlog.h`

### Handled records

`XLOG_HASH_INIT_META_PAGE`, `XLOG_HASH_INIT_BITMAP_PAGE`,
`XLOG_HASH_INSERT`, `XLOG_HASH_ADD_OVFL_PAGE`,
`XLOG_HASH_SPLIT_ALLOCATE_PAGE`, `XLOG_HASH_SPLIT_PAGE`,
`XLOG_HASH_SPLIT_COMPLETE`, `XLOG_HASH_MOVE_PAGE_CONTENTS`,
`XLOG_HASH_SQUEEZE_PAGE`, `XLOG_HASH_DELETE`,
`XLOG_HASH_UPDATE_META_PAGE`, `XLOG_HASH_VACUUM_ONE_PAGE`.

### State mutations

Hash index pages.

### Hot-standby behavior

`XLOG_HASH_VACUUM_ONE_PAGE` emits
`ResolveRecoveryConflictWithSnapshot(latestRemovedXid)` — same
mechanism as btree DELETE/REUSE_PAGE.

### Idempotency / LSN-skip

All paths through `XLogReadBufferForRedo`.

---

## `gin_redo` — RM_GIN_ID = 13

* **redo function**: `gin_redo` at
  `src/backend/access/gin/ginxlog.c:726`
* **rm_startup**: `gin_xlog_startup`
* **rm_cleanup**: `gin_xlog_cleanup`
* **header**: `src/include/access/ginxlog.h`

### Handled records

`XLOG_GIN_CREATE_PTREE`, `XLOG_GIN_INSERT`, `XLOG_GIN_SPLIT`,
`XLOG_GIN_VACUUM_PAGE`, `XLOG_GIN_VACUUM_DATA_LEAF_PAGE`,
`XLOG_GIN_DELETE_PAGE`, `XLOG_GIN_UPDATE_META_PAGE`,
`XLOG_GIN_INSERT_LISTPAGE`, `XLOG_GIN_DELETE_LISTPAGE`.

### State mutations

GIN index pages, posting trees/lists.

### Hot-standby behavior

No direct conflict — GIN VACUUM relies on the heap-level
`XLOG_HEAP2_PRUNE_*` records to issue snapshot conflicts.

### Incomplete-split tracker

Like btree, GIN tracks incomplete splits via the rm_startup/
rm_cleanup hooks.

---

## `gist_redo` — RM_GIST_ID = 14

* **redo function**: `gist_redo` at
  `src/backend/access/gist/gistxlog.c:397`
* **rm_startup**: `gist_xlog_startup`
* **rm_cleanup**: `gist_xlog_cleanup`
* **header**: `src/include/access/gistxlog.h`

### Handled records

`XLOG_GIST_PAGE_UPDATE`, `XLOG_GIST_DELETE`,
`XLOG_GIST_PAGE_REUSE`, `XLOG_GIST_PAGE_SPLIT`,
`XLOG_GIST_ASSIGN_LSN`, `XLOG_GIST_PAGE_DELETE`.

### State mutations

GiST pages.

### Hot-standby behavior

`XLOG_GIST_PAGE_REUSE` emits snapshot-conflict horizon via
`ResolveRecoveryConflictWithSnapshotFullXid` — index page being
reused had references that an old snapshot might still be using.

---

## `spg_redo` — RM_SPGIST_ID = 16

* **redo function**: `spg_redo` at
  `src/backend/access/spgist/spgxlog.c:935`
* **rm_startup**: `spg_xlog_startup`
* **rm_cleanup**: `spg_xlog_cleanup`
* **header**: `src/include/access/spgxlog.h`

### Handled records

`XLOG_SPGIST_ADD_LEAF`, `XLOG_SPGIST_MOVE_LEAFS`,
`XLOG_SPGIST_ADD_NODE`, `XLOG_SPGIST_SPLIT_TUPLE`,
`XLOG_SPGIST_PICKSPLIT`, `XLOG_SPGIST_VACUUM_LEAF`,
`XLOG_SPGIST_VACUUM_ROOT`, `XLOG_SPGIST_VACUUM_REDIRECT`.

### State mutations

SP-GiST pages, redirect tombstone state.

### Hot-standby behavior

`XLOG_SPGIST_VACUUM_REDIRECT` emits snapshot-conflict horizon —
SP-GiST uses redirect tombstones to handle concurrent
vacuum/scan; replaying the cleanup of a tombstone is unsafe for
old snapshots.

---

## `brin_redo` — RM_BRIN_ID = 17

* **redo function**: `brin_redo` at
  `src/backend/access/brin/brin_xlog.c:309`
* **header**: `src/include/access/brin_xlog.h`

### Handled records

`XLOG_BRIN_CREATE_INDEX`, `XLOG_BRIN_INSERT`,
`XLOG_BRIN_UPDATE`, `XLOG_BRIN_SAMEPAGE_UPDATE`,
`XLOG_BRIN_REVMAP_EXTEND`, `XLOG_BRIN_DESUMMARIZE`.

### State mutations

BRIN regular pages, BRIN revmap pages.

### Hot-standby behavior

No direct conflict — BRIN summary updates don't invalidate
visibility because BRIN entries are summary data, not tuple
versions.

---

## Common pattern

Every index AM redo callback follows the same skeleton:

```c
static void
amxx_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    /* For records that may invalidate snapshots */
    if (info needs snapshot conflict)
        ResolveRecoveryConflictWithSnapshot(horizon, ...);

    switch (info) {
        case XLOG_AMXX_OP1: amxx_xlog_op1(record); break;
        case XLOG_AMXX_OP2: amxx_xlog_op2(record); break;
        ...
        default: elog(PANIC, "amxx_redo: unknown op code %u", info);
    }
}

static void amxx_xlog_op1(XLogReaderState *record)
{
    Buffer buf;
    if (XLogReadBufferForRedo(record, 0, &buf) == BLK_NEEDS_REDO) {
        Page page = BufferGetPage(buf);
        /* op-specific page surgery */
        PageSetLSN(page, record->EndRecPtr);
        MarkBufferDirty(buf);
    }
    if (BufferIsValid(buf)) UnlockReleaseBuffer(buf);
}
```

All AMs except BRIN emit at least one snapshot-conflict path; all
AMs except hash and BRIN have rm_startup/rm_cleanup for
incomplete-operation tracking.

---

## Source references

* `src/backend/access/hash/hash_xlog.c:1067` — `hash_redo`
* `src/backend/access/gin/ginxlog.c:726` — `gin_redo`
* `src/backend/access/gist/gistxlog.c:397` — `gist_redo`
* `src/backend/access/spgist/spgxlog.c:935` — `spg_redo`
* `src/backend/access/brin/brin_xlog.c:309` — `brin_redo`
* Headers in `src/include/access/{hash,gin,gist,spg,brin}_xlog.h`
