# 14 — Resource Manager Dispatch

[← Promotion and End of Recovery](13_promotion_and_end_of_recovery.md) | [index](index.md) | [next: Recovery Buffer Helpers →](15_recovery_buffer_helpers.md)

---


The Resource Manager (rmgr) layer is PostgreSQL's redo dispatch
mechanism. Every WAL record carries an 8-bit `xl_rmid` identifying
which rmgr owns the record; `ApplyWalRecord` looks up the rmgr's
`rm_redo` callback in the `RmgrTable[]` and invokes it.


## Architecture

```mermaid
graph TB
  AWR[ApplyWalRecord<br/>xlogrecovery.c:1908] -->|GetRmgr rmid| RT[(RmgrTable[]<br/>rmgr.c)]
  RT -->|RM_XLOG_ID=0| XL[xlog_redo]
  RT -->|RM_XACT_ID=1| XA[xact_redo]
  RT -->|RM_SMGR_ID=2| SM[smgr_redo]
  RT -->|RM_CLOG_ID=3| CL[clog_redo]
  RT -->|RM_DBASE_ID=4| DB[dbase_redo]
  RT -->|RM_TBLSPC_ID=5| TS[tblspc_redo]
  RT -->|RM_MULTIXACT_ID=6| MX[multixact_redo]
  RT -->|RM_RELMAP_ID=7| RM[relmap_redo]
  RT -->|RM_STANDBY_ID=8| SB[standby_redo]
  RT -->|RM_HEAP2_ID=9| H2[heap2_redo]
  RT -->|RM_HEAP_ID=10| HE[heap_redo]
  RT -->|RM_BTREE_ID=11| BT[btree_redo]
  RT -->|RM_HASH_ID=12| HA[hash_redo]
  RT -->|RM_GIN_ID=13| GI[gin_redo]
  RT -->|RM_GIST_ID=14| GS[gist_redo]
  RT -->|RM_SEQ_ID=15| SQ[seq_redo]
  RT -->|RM_SPGIST_ID=16| SP[spg_redo]
  RT -->|RM_BRIN_ID=17| BR[brin_redo]
  RT -->|RM_COMMIT_TS_ID=18| CT[commit_ts_redo]
  RT -->|RM_REPLORIGIN_ID=19| RO[replorigin_redo]
  RT -->|RM_GENERIC_ID=20| GE[generic_redo]
  RT -->|RM_LOGICALMSG_ID=21| LM[logicalmsg_redo]
  RT -.->|RM_CUSTOM_ID 128..255| CR[Custom rmgrs<br/>via RegisterCustomRmgr]
```

## `RmgrData` (`src/include/access/xlog_internal.h`)

The method-table struct, one per rmgr:

```c
typedef struct RmgrData
{
    const char *rm_name;
    void        (*rm_redo) (XLogReaderState *record);
    void        (*rm_desc) (StringInfo buf, XLogReaderState *record);
    const char *(*rm_identify) (uint8 info);
    void        (*rm_startup) (void);
    void        (*rm_cleanup) (void);
    bool        (*rm_mask) (char *pagedata, BlockNumber blkno);
    void        (*rm_decode) (struct LogicalDecodingContext *ctx,
                              struct XLogRecordBuffer *buf);
} RmgrData;
```

Field meaning:

* `rm_redo` — apply the record (recovery side).
* `rm_desc` / `rm_identify` — pretty-print for `pg_waldump`.
* `rm_startup` / `rm_cleanup` — optional one-shot hooks called by
  `RmgrStartup` / `RmgrCleanup` around the redo loop. Currently
  used by `btree`, `gin`, `gist`, `spgist` to track incomplete
  splits.
* `rm_mask` — for `wal_consistency_checking`: masks volatile fields
  (LSN, hint bits) before comparing pages.
* `rm_decode` — logical-decoding callback (logical replication).

## `RmgrTable` (`src/backend/access/transam/rmgr.c`)

```c
const RmgrData RmgrTable[RM_MAX_ID + 1] = {
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask,decode) \
    { name, redo, desc, identify, startup, cleanup, mask, decode },
#include "access/rmgrlist.h"
#undef PG_RMGR
};
```

The macro `PG_RMGR(...)` is defined in `src/include/access/rmgrlist.h`,
which is the single master table:

```c
/* rmgrlist.h excerpt */
PG_RMGR(RM_XLOG_ID,      "XLOG",      xlog_redo,      xlog_desc,      xlog_identify,      NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_XACT_ID,      "Transaction", xact_redo,    xact_desc,      xact_identify,      NULL,                NULL,             NULL,                xact_decode)
PG_RMGR(RM_SMGR_ID,      "Storage",   smgr_redo,      smgr_desc,      smgr_identify,      NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_CLOG_ID,      "CLOG",      clog_redo,      clog_desc,      clog_identify,      NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_DBASE_ID,     "Database",  dbase_redo,     dbase_desc,     dbase_identify,     NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_TBLSPC_ID,    "Tablespace",tblspc_redo,    tblspc_desc,    tblspc_identify,    NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_MULTIXACT_ID, "MultiXact", multixact_redo, multixact_desc, multixact_identify, NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_RELMAP_ID,    "RelMap",    relmap_redo,    relmap_desc,    relmap_identify,    NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_STANDBY_ID,   "Standby",   standby_redo,   standby_desc,   standby_identify,   NULL,                NULL,             NULL,                standby_decode)
PG_RMGR(RM_HEAP2_ID,     "Heap2",     heap2_redo,     heap2_desc,     heap2_identify,     NULL,                NULL,             heap_mask,           heap2_decode)
PG_RMGR(RM_HEAP_ID,      "Heap",      heap_redo,      heap_desc,      heap_identify,      NULL,                NULL,             heap_mask,           heap_decode)
PG_RMGR(RM_BTREE_ID,     "Btree",     btree_redo,     btree_desc,     btree_identify,     btree_xlog_startup,  btree_xlog_cleanup, btree_mask,        NULL)
PG_RMGR(RM_HASH_ID,      "Hash",      hash_redo,      hash_desc,      hash_identify,      NULL,                NULL,             hash_mask,           NULL)
PG_RMGR(RM_GIN_ID,       "Gin",       gin_redo,       gin_desc,       gin_identify,       gin_xlog_startup,    gin_xlog_cleanup, gin_mask,            NULL)
PG_RMGR(RM_GIST_ID,      "Gist",      gist_redo,      gist_desc,      gist_identify,      gist_xlog_startup,   gist_xlog_cleanup, gist_mask,          NULL)
PG_RMGR(RM_SEQ_ID,       "Sequence",  seq_redo,       seq_desc,       seq_identify,       NULL,                NULL,             seq_mask,            NULL)
PG_RMGR(RM_SPGIST_ID,    "SPGist",    spg_redo,       spg_desc,       spg_identify,       spg_xlog_startup,    spg_xlog_cleanup, spg_mask,            NULL)
PG_RMGR(RM_BRIN_ID,      "BRIN",      brin_redo,      brin_desc,      brin_identify,      NULL,                NULL,             brin_mask,           NULL)
PG_RMGR(RM_COMMIT_TS_ID, "CommitTs",  commit_ts_redo, commit_ts_desc, commit_ts_identify, NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_REPLORIGIN_ID,"ReplicationOrigin", replorigin_redo, replorigin_desc, replorigin_identify, NULL, NULL, NULL,            NULL)
PG_RMGR(RM_GENERIC_ID,   "Generic",   generic_redo,   generic_desc,   generic_identify,   NULL,                NULL,             generic_mask,        NULL)
PG_RMGR(RM_LOGICALMSG_ID,"LogicalMessage", logicalmsg_redo, logicalmsg_desc, logicalmsg_identify, NULL, NULL, NULL,               logicalmsg_decode)
```

The order in `rmgrlist.h` is the source of truth for RM_*_ID
numeric values: `RM_XLOG_ID=0`, `RM_XACT_ID=1`, …, `RM_LOGICALMSG_ID=21`.

## `GetRmgr` (`rmgr.c`, importance 0.60)

```c
static inline const RmgrData *
GetRmgr(RmgrId rmid)
{
    if (unlikely(!RmgrIdIsBuiltin(rmid) && !RmgrIdExists(rmid)))
        ereport(PANIC, ..., "resource manager with ID %d not registered", rmid);
    return &RmgrTable[rmid];
}
```

Inline lookup with bounds check. Custom rmgrs (rmid 128..255) are
slotted into `RmgrTable` by `RegisterCustomRmgr`.

## `RmgrStartup` and `RmgrCleanup`

Called once around the redo loop:

```c
void RmgrStartup(void)
{
    for (int rmid = 0; rmid <= RM_MAX_ID; rmid++)
        if (RmgrTable[rmid].rm_startup != NULL)
            RmgrTable[rmid].rm_startup();
}

void RmgrCleanup(void)
{
    for (int rmid = 0; rmid <= RM_MAX_ID; rmid++)
        if (RmgrTable[rmid].rm_cleanup != NULL)
            RmgrTable[rmid].rm_cleanup();
}
```

Currently used by:

* `btree` — initializes/cleans up the incomplete-split tracker.
* `gin` — same (incomplete-split tracker).
* `gist` — same.
* `spgist` — same.

The pattern: btree/gin/gist/spgist record incomplete splits in a
hash table during redo (a leaf split was logged but the parent
update record is still ahead of us). At `rm_cleanup`, any leftover
incomplete splits are completed by walking the index manually.
This is necessary because a crash mid-split would otherwise leave
the index in an inconsistent state.

---

## Custom rmgrs (`RegisterCustomRmgr`)

Extension point for out-of-tree redo dispatch:

```c
void RegisterCustomRmgr(RmgrId rmid, const RmgrData *rmgr);
```

Called from an extension's `_PG_init`. The extension provides its
own `rm_redo` for records it has previously inserted via
`XLogInsert(rmid, ...)`. Used by Neon, Citus, and similar
extensions that need to log custom data to the WAL.

`RM_MIN_CUSTOM_ID = 128`, `RM_MAX_CUSTOM_ID = 255`. Built-in IDs
are 0..21, so there's room for many custom rmgrs.

---

## `rmgrdesc` plugins

`pg_waldump` uses the `rm_desc` and `rm_identify` callbacks to
pretty-print records. For custom rmgrs, the extension can provide
a `rmgrdesc` plugin shared library; pg_waldump's `--rmgr=<name>`
flag selects which to use.

---

## Source references

* `src/backend/access/transam/rmgr.c` — `RmgrTable`, `GetRmgr`,
  `RegisterCustomRmgr`, `RmgrStartup`, `RmgrCleanup`
* `src/include/access/rmgrlist.h` — master `PG_RMGR(...)` table
* `src/include/access/xlog_internal.h` — `RmgrData`,
  `RmgrId`, `RM_*_ID` constants

## Related catalog

For the per-callback details of all 22 built-in redo functions, see
`redo_callback_catalog/`:

* [core_xlog_xact_redo.md](17_redo_callback_catalog.md)
* [storage_smgr_dbase_tblspc_redo.md](17_redo_callback_catalog.md)
* [slru_redo.md](17_redo_callback_catalog.md)
* [standby_redo.md](17_redo_callback_catalog.md#9-standby_redo--rm_standby_id--8)
* [heap_redo.md](17_redo_callback_catalog.md)
* [btree_index_redo.md](17_redo_callback_catalog.md#12-btree_redo--rm_btree_id--11)
* [hash_gin_gist_spg_brin_redo.md](17_redo_callback_catalog.md)
* [seq_replorigin_generic_logicalmsg_redo.md](17_redo_callback_catalog.md)
