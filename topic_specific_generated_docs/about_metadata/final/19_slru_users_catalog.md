# 19 — SLRU Users Catalog

[Up: index.md](index.md)  |  [Prev: 18 Catalog Inventory](18_catalog_inventory.md)  |  [Next: 20 WAL Record Catalog](20_wal_record_catalog.md)

## Prerequisites

- [08 SLRU Framework](08_slru_framework.md) — every section here uses the same vocabulary.

This chapter is a per-instance reference for every SLRU in PostgreSQL.
Each entry follows a standardized template:

- **Identity**: `SlruCtl` C identifier and on-disk directory.
- **Per-page layout**: entry size and entries-per-page.
- **Page-number formula**: how a logical key maps to a page number.
- **Bank-lock partitioning**: which GUC sets `nslots`.
- **Bootstrap path**: the `BootStrap*` and `*ShmemInit` calls.
- **Recovery path**: the `Startup*` / `Trim*` hooks.
- **Checkpoint hook**: which `CheckPoint*` function is called.
- **Extend / Truncate**: how new pages are added and old ones removed.
- **WAL records**: which info bytes apply.
- **Wraparound considerations**: special cases.
- **Retention**: when pages are eligible for truncation.

Total: 7 SLRU instances. Quick reference in [appendix_slru_quick_reference.md](appendix_slru_quick_reference.md).

## SLRU: CLOG (XactCtl, pg_xact)

### Identity

- **SlruCtl pointer**: `XactCtl` (alias for `&XactCtlData`)
- **On-disk directory**: `$PGDATA/pg_xact/`
- **Source**: `src/backend/access/transam/clog.c`

### Per-page layout

- **Entry size**: 2 bits per XID (XidStatus).
- **Entries per page**: `CLOG_XACTS_PER_PAGE = BLCKSZ * CLOG_XACTS_PER_BYTE
  = 8192 * 4 = 32768`.
- **Total per page**: 32768 XIDs in 8 KiB.

### Page-number formula

```c
TransactionIdToPage(xid) = xid / CLOG_XACTS_PER_PAGE
```

Within a page:
- byte index = `(xid % CLOG_XACTS_PER_PAGE) / CLOG_XACTS_PER_BYTE`
- bit-pair  = `(xid % CLOG_XACTS_PER_BYTE) * 2`

### Bank-lock partitioning

`SimpleLruGetBankLock(XactCtl, pageno) = bank_locks[pageno % nbanks]` where
`nbanks = nslots / 16`. Default `nslots` from `xact_buffers` GUC (-1 means
auto-tune).

### Bootstrap path

- `BootStrapCLOG()`: zero page 0, write it, fsync.
- `CLOGShmemInit()`: `SimpleLruInit(XactCtl, "Xact", nslots,
  CLOG_LSNS_PER_PAGE, "pg_xact", ..., SYNC_HANDLER_CLOG, false)` at
  `clog.c:811`.

### Recovery path

- `StartupCLOG()`: set `latest_page_number = TransactionIdToPage(nextXid - 1)`.
- `TrimCLOG()`: zero the trailing portion of the live page beyond `nextXid`.

### Checkpoint hook

```c
void CheckPointCLOG(void) { SimpleLruWriteAll(XactCtl, true); }
```

Flush every dirty page; sync requests are queued for the checkpointer.

### Extend / Truncate

- **Extend**: `ExtendCLOG(newestXact)` from `GetNewTransactionId`. Emits
  `XLOG_CLOG_ZEROPAGE` and `SimpleLruZeroPage` when the new XID falls on
  a fresh page.
- **Truncate**: `TruncateCLOG(oldestXact, oldestxid_datoid)` from
  `vac_truncate_clog`. Emits `XLOG_CLOG_TRUNCATE` (xl_clog_truncate
  payload), advances `ShmemVariableCache->oldestClogXid`, calls
  `SimpleLruTruncate`.

### WAL records

| Record name           | info | redo function | payload          |
|-----------------------|------|---------------|------------------|
| XLOG_CLOG_ZEROPAGE    | 0x00 | clog_redo     | int64 pageno     |
| XLOG_CLOG_TRUNCATE    | 0x10 | clog_redo     | xl_clog_truncate |

`clog_redo` lives at `src/backend/access/transam/clog.c:1107`.

### Wraparound considerations

CLOG covers the entire `2^32` XID space. The `PagePrecedes` callback uses
modular arithmetic:

```c
static bool CLOGPagePrecedes(int64 page1, int64 page2)
{
    TransactionId xid1 = page1 * CLOG_XACTS_PER_PAGE;
    xid1 += (CLOG_XACTS_PER_PAGE / 2);
    TransactionId xid2 = page2 * CLOG_XACTS_PER_PAGE;
    xid2 += (CLOG_XACTS_PER_PAGE / 2);
    return TransactionIdPrecedes(xid1, xid2) &&
           TransactionIdPrecedes(xid1, xid2 + (CLOG_XACTS_PER_PAGE - 1));
}
```

This ensures that, at a wraparound boundary, "older" pages are correctly
identified for SimpleLruTruncate.

### Retention

CLOG entries are kept until cluster-wide `oldestXid` (= `min(datfrozenxid)`
across pg_database) advances past the page. Vacuum's freezing logic drives
this advance.

### Group commit (TransactionGroupUpdateXidStatus)

When N committers concurrently target the same CLOG page, `TransactionIdSetPageStatus`
falls back to a per-page leader-follower queue: one leader acquires the
bank-lock, processes its own + all queued requests in one critical section,
and signals the followers. Converts O(N) lock acquisitions into O(1) for
hot pages. Defined in `clog.c::TransactionGroupUpdateXidStatus`.

### Async commit and group_lsn

`SimpleLruInit` is called with `nlsns = CLOG_LSNS_PER_PAGE = 1024` (the only
SLRU with a non-zero `nlsns`). Each page has 1024 LSN slots, one per group
of 32 consecutive XIDs. `TransactionIdSetTreeStatus` propagates the commit
LSN into the appropriate slot. `SimpleLruWritePage` then calls
`XLogFlush(max_lsn_in_slot_range)` before the page write.

### Cross-references

- `[09 CLOG](09_clog.md)` — full deep dive.
- `[08 SLRU Framework](08_slru_framework.md)` — SLRU machinery.
- `[20 WAL Record Catalog § CLOG](20_wal_record_catalog.md)` — WAL record details.


## SLRU: SUBTRANS (SubTransCtl, pg_subtrans)

### Identity

- **SlruCtl pointer**: `SubTransCtl`
- **On-disk directory**: `$PGDATA/pg_subtrans/`
- **Source**: `src/backend/access/transam/subtrans.c`

### Per-page layout

- **Entry size**: 4 bytes per XID (TransactionId — the parent's XID).
- **Entries per page**: `SUBTRANS_XACTS_PER_PAGE = BLCKSZ / 4 = 2048`.
- **Per-page total**: 2048 (xid → parent_xid) entries.

### Page-number formula

```c
TransactionIdToPage(xid)  = xid / SUBTRANS_XACTS_PER_PAGE
TransactionIdToEntry(xid) = xid % SUBTRANS_XACTS_PER_PAGE
```

### Bank-lock partitioning

Same as other SLRUs: `bank_locks[pageno % nbanks]`. Default `nslots` from
`subtransaction_buffers` GUC.

### Bootstrap path

- `BootStrapSUBTRANS()`: zero page 0.
- `SUBTRANSShmemInit()`: `SimpleLruInit(SubTransCtl, "Subtrans", ...,
  "pg_subtrans", ..., SYNC_HANDLER_NONE, false)` at `subtrans.c:244`.

Note: `SYNC_HANDLER_NONE` — pg_subtrans is **not fsynced** at checkpoint.
Crash safety comes from runtime reconstruction.

### Recovery path

- `StartupSUBTRANS(oldestActiveXID)`: zero every page from
  `TransactionIdToPage(oldestActiveXID)` through the page containing
  `nextXid`. The contents from before the crash are discarded.

### Checkpoint hook

```c
void CheckPointSUBTRANS(void) { SimpleLruWriteAll(SubTransCtl, true); }
```

Flushes dirty pages but does NOT issue sync requests (sync handler is NONE).

### Extend / Truncate

- **Extend**: implicit, when `SubTransSetParent` writes to a page that
  doesn't exist, `SimpleLruZeroPage` allocates the slot. **No WAL is
  emitted** (this is the key SUBTRANS-vs-CLOG difference).
- **Truncate**: `TruncateSUBTRANS(oldestXact)` from `vac_truncate_clog`.
  Drops segments older than the cutoff. **No WAL emitted.**

### WAL records

**None.** This is the entire premise of SUBTRANS as a runtime-reconstructable
metadata structure.

Why is this safe? Because:
1. Visibility checks during replay rely on the `subxacts[]` array embedded
   in `xl_xact_commit` / `xl_xact_abort`, not on pg_subtrans.
2. After replay, parent links for in-flight (not-yet-finalized) transactions
   are reconstructed by re-running `SubTransSetParent` from the new
   AssignTransactionId calls.
3. The TransactionXmin floor in `SubTransGetTopmostTransaction` ensures no
   reader walks below a known-safe XID.

### Wraparound considerations

The same modular `PagePrecedes` callback. Wraparound is handled by
truncation in lockstep with CLOG truncation.

### Retention

Pages older than `vac_truncate_clog`'s cutoff are removed by
`TruncateSUBTRANS`.

### Cross-references

- `[10 SUBTRANS](10_subtrans.md)` — full deep dive.
- `[08 SLRU Framework](08_slru_framework.md)` — SLRU machinery.


## SLRU: MultiXact Offsets (MultiXactOffsetCtl, pg_multixact/offsets)

### Identity

- **SlruCtl pointer**: `MultiXactOffsetCtl`
- **On-disk directory**: `$PGDATA/pg_multixact/offsets/`
- **Source**: `src/backend/access/transam/multixact.c`

### Per-page layout

- **Entry size**: 4 bytes per MultiXactId (`MultiXactOffset` = uint32).
- **Entries per page**: `MULTIXACT_OFFSETS_PER_PAGE =
  BLCKSZ / sizeof(MultiXactOffset) = 2048`.

Each entry is the absolute offset (in members units) into the
`pg_multixact/members` SLRU where this multi's first member lives.

### Page-number formula

```c
MultiXactIdToOffsetPage(multi)  = multi / MULTIXACT_OFFSETS_PER_PAGE
MultiXactIdToOffsetEntry(multi) = multi % MULTIXACT_OFFSETS_PER_PAGE
```

### Bank-lock partitioning

`bank_locks[pageno % nbanks]`; default `nslots` from
`multixact_offset_buffers` GUC.

### Bootstrap path

- `BootStrapMultiXact()`: zero page 0 of both offsets and members SLRUs.
- `MultiXactShmemInit()`: `SimpleLruInit(MultiXactOffsetCtl, "MultiXactOffset",
  nslots, 0, "pg_multixact/offsets", ..., SYNC_HANDLER_MULTIXACT_OFFSET,
  false)` at `multixact.c:1965`.

### Recovery path

- `StartupMultiXact()`:
  - read `nextMulti`, `nextMultiOffset` from `ControlFile->checkPointCopy`.
  - `MultiXactSetNextMXact(nextMulti, nextMultiOffset)`.
  - set `latest_page_number = MultiXactIdToOffsetPage(nextMulti - 1)`.
- `TrimMultiXact()`: zero the trailing portion of the live offsets page.

### Checkpoint hook

```c
void CheckPointMultiXact(void)
{
    SimpleLruWriteAll(MultiXactOffsetCtl, true);
    SimpleLruWriteAll(MultiXactMemberCtl, true);
    /* ... update ControlFile cursors ... */
}
```

### Extend / Truncate

- **Extend**: implicit when `GetNewMultiXactId` advances onto a fresh page;
  emits `XLOG_MULTIXACT_ZERO_OFF_PAGE` and `SimpleLruZeroPage`.
- **Truncate**: `TruncateMultiXact(newOldestMulti, newOldestMultiDB)` from
  `vac_truncate_clog`. Emits `XLOG_MULTIXACT_TRUNCATE_ID` (drives both
  offsets and members truncation).

### WAL records

| info | name                          | payload              |
|------|-------------------------------|----------------------|
| 0x00 | XLOG_MULTIXACT_ZERO_OFF_PAGE  | int64 pageno         |
| 0x20 | XLOG_MULTIXACT_CREATE_ID      | xl_multixact_create  |
| 0x30 | XLOG_MULTIXACT_TRUNCATE_ID    | xl_multixact_truncate|

`multixact_redo` dispatches.

### Wraparound considerations

MultiXactId is 32-bit and wraps at `2^32`. The thresholds:
- `multiVacLimit`: trigger emergency vacuum.
- `multiWarnLimit`: log warning.
- `multiStopLimit`: refuse new multi allocation.

These are computed in `SetMultiXactIdLimit` from `pg_control.oldestMulti`.
Vacuum advances `oldestMulti` by computing `min(datminmxid)` across
pg_database.

### Retention

Pages with all multis older than `oldestMulti` are truncated.

### Cross-references

- `[12 MultiXact](12_multixact.md)` — full deep dive.
- `[Section: MultiXact Members](#slru-multixact-members-multixactmemberctl-pg_multixactmembers)` — companion SLRU.
- `[20 WAL Record Catalog § MultiXact](20_wal_record_catalog.md)`.


## SLRU: MultiXact Members (MultiXactMemberCtl, pg_multixact/members)

### Identity

- **SlruCtl pointer**: `MultiXactMemberCtl`
- **On-disk directory**: `$PGDATA/pg_multixact/members/`
- **Source**: `src/backend/access/transam/multixact.c`
- **Long segment names**: YES (`long_segment_names = true`) — because the
  members offset can grow well beyond the 32-bit segment-name range that
  short names support.

### Per-page layout

- **Entry size**: variable. Members are stored in groups of 4:
  - 1 byte of "flags" packing 4 × 2-bit `MultiXactStatus` values (8 bits used).
  - 4 × 4 bytes for the four `TransactionId` xids.
  - Total per group: 17 bytes for 4 entries.
- **Entries per page**: roughly `(BLCKSZ - SizeOfPageHeaderData) / 4.25 ≈ 1635`.
  The exact constant `MULTIXACT_MEMBERS_PER_PAGE` is computed from
  `MULTIXACT_MEMBER_SAFE_MULTIPLIER = 5` to leave room for partial groups.

Layout helpers (multixact.c):
```c
#define MXOffsetToFlagsOffset(offset)   /* page-byte offset of the flags byte */
#define MXOffsetToFlagsBitShift(offset) /* shift within the flag byte */
#define MXOffsetToMemberOffset(offset)  /* byte offset of the TransactionId */
#define MXOffsetToMemberPage(offset)    ((offset) / MULTIXACT_MEMBERS_PER_PAGE)
```

### Page-number formula

`MXOffsetToMemberPage(offset) = offset / MULTIXACT_MEMBERS_PER_PAGE`.

`offset` is the absolute member-number, stored in `pg_multixact/offsets`.

### Bank-lock partitioning

Same scheme; default `nslots` from `multixact_member_buffers` GUC.

### Bootstrap path

`SimpleLruInit(MultiXactMemberCtl, "MultiXactMember", nslots, 0,
"pg_multixact/members", ..., SYNC_HANDLER_MULTIXACT_MEMBER, true)`
at `multixact.c:1972`.

### Recovery path

`StartupMultiXact` and `TrimMultiXact` cover both offsets and members.
Members trimming zeroes the trailing portion of the live members page beyond
`nextMultiOffset`.

### Checkpoint hook

`CheckPointMultiXact` (shared with offsets).

### Extend / Truncate

- **Extend**: implicit when `RecordNewMultiXact` writes onto a fresh page;
  emits `XLOG_MULTIXACT_ZERO_MEM_PAGE`.
- **Truncate**: `TruncateMultiXact` truncates members up to the offset
  belonging to `newOldestMulti`. Driven by the same WAL record
  `XLOG_MULTIXACT_TRUNCATE_ID`.

### WAL records

| info | name                          | payload              |
|------|-------------------------------|----------------------|
| 0x10 | XLOG_MULTIXACT_ZERO_MEM_PAGE  | int64 pageno         |
| 0x20 | XLOG_MULTIXACT_CREATE_ID      | xl_multixact_create  |
| 0x30 | XLOG_MULTIXACT_TRUNCATE_ID    | xl_multixact_truncate|

(0x20 and 0x30 are also documented under offsets — they affect both SLRUs.)

### Wraparound considerations

The member offset is also 32-bit and wraps independently of the multi-id
counter. A multi with many members (e.g., a popular row locked by 100
distinct transactions) consumes 100 member offsets. The
`MultiXactMemberFreezeThreshold` function approximates "average members per
multi" and is used to gate vacuum's freezing aggressiveness.

If members run out before multis: `GetNewMultiXactId` ereports an error
referencing `multixact members exhausted`. Operators must run aggressive
vacuum to advance `oldestMulti`.

### Retention

Members older than the offset belonging to `oldestMulti` are truncated.

### Cross-references

- `[12 MultiXact](12_multixact.md)` — full deep dive, especially wraparound.
- `[Section: MultiXact Offsets](#slru-multixact-offsets-multixactoffsetctl-pg_multixactoffsets)` — companion SLRU.
- `[20 WAL Record Catalog § MultiXact](20_wal_record_catalog.md)`.


## SLRU: CommitTs (CommitTsCtl, pg_commit_ts)

### Identity

- **SlruCtl pointer**: `CommitTsCtl`
- **On-disk directory**: `$PGDATA/pg_commit_ts/`
- **Source**: `src/backend/access/transam/commit_ts.c`

### Per-page layout

```c
typedef struct CommitTimestampEntry {
    TimestampTz time;            /* 8 bytes */
    RepOriginId nodeid;          /* 2 bytes */
} CommitTimestampEntry;          /* 10 bytes total */
```

- **Entry size**: 10 bytes.
- **Entries per page**: `COMMIT_TS_XACTS_PER_PAGE = BLCKSZ / 10 = 819`.

### Page-number formula

```c
TransactionIdToCTsPage(xid)  = xid / COMMIT_TS_XACTS_PER_PAGE
TransactionIdToCTsEntry(xid) = xid % COMMIT_TS_XACTS_PER_PAGE
```

The byte offset in the page is `entry * 10`.

### Bank-lock partitioning

`bank_locks[pageno % nbanks]`; default `nslots` from
`commit_timestamp_buffers` GUC.

### Bootstrap path

- `BootStrapCommitTs()`: zero page 0 (only if `track_commit_timestamp = on`
  at initdb).
- `CommitTsShmemInit()`: `SimpleLruInit(CommitTsCtl, "CommitTs", nslots, 0,
  "pg_commit_ts", ..., SYNC_HANDLER_COMMIT_TS, false)` at
  `commit_ts.c:556`.

### Recovery path

- `StartupCommitTs()`:
  - read `oldestCommitTsXid`, `newestCommitTsXid` from `ControlFile`.
  - set `latest_page_number = TransactionIdToCTsPage(newestCommitTsXid)`.
  - load `dataLastCommit` from disk (the (xid, ts, origin) of the most
    recent commit).

### Checkpoint hook

```c
void CheckPointCommitTs(void) { SimpleLruWriteAll(CommitTsCtl, true); }
```

No-op when `track_commit_timestamp` is off (no dirty pages).

### Extend / Truncate

- **Extend**: when `GetNewTransactionId` advances onto a fresh CommitTs
  page, emits `XLOG_COMMIT_TS_ZEROPAGE`.
- **Truncate**: `TruncateCommitTs(oldestXact)` from `vac_truncate_clog`.
  Emits `XLOG_COMMIT_TS_TRUNCATE` (xl_commit_ts_truncate payload).

### WAL records

| info | name                    | payload                | redo                |
|------|-------------------------|------------------------|---------------------|
| 0x00 | XLOG_COMMIT_TS_ZEROPAGE | int64 pageno           | commit_ts_redo      |
| 0x10 | XLOG_COMMIT_TS_TRUNCATE | xl_commit_ts_truncate  | commit_ts_redo      |
| 0x40 | XLOG_COMMIT_TS_SETTS    | xl_commit_ts_set       | commit_ts_redo      |

`commit_ts_redo` lives at `commit_ts.c:1023`.

### SETTS special case

`XLOG_COMMIT_TS_SETTS` is emitted only when the (xid → ts, origin) pair
includes a non-default `nodeid` (i.e., logical replication origin
attribution). The default-nodeid case is recorded by the standby's redo
of `xact_redo_commit`, which calls `TransactionTreeSetCommitTsData` with
the timestamp embedded in `xl_xact_commit`.

### Wraparound considerations

CommitTs entries cover the same 32-bit XID space as CLOG. The same modular
`PagePrecedes` callback. CommitTs is allowed to be sparser than CLOG (gaps
appear when track_commit_timestamp toggles off and on).

### Retention

Pages older than `oldestCommitTsXid` are truncated. The advance happens
during `vac_truncate_clog` after `oldestXid` advances.

### Cross-references

- `[11 Commit Timestamps](11_commit_timestamps.md)` — full deep dive.
- `[20 WAL Record Catalog § CommitTs](20_wal_record_catalog.md)`.


## SLRU: Other SLRUs: Notify and Serial

These two SLRUs are not strictly metadata — they support specific runtime
features — but they share the SLRU framework and merit a brief inventory
entry.

### Notify (LISTEN/NOTIFY)

#### Identity

- **SlruCtl pointer**: `NotifyCtl`
- **On-disk directory**: `$PGDATA/pg_notify/`  (note: not under PGDATA's
  global/ or base/, but at PGDATA root)
- **Source**: `src/backend/commands/async.c`

#### Per-page layout

Variable-length `AsyncQueueEntry` records, each containing:

```c
typedef struct AsyncQueueEntry
{
    int                 length;          /* total entry length, in bytes */
    Oid                 dboid;
    TransactionId       xid;
    ProcNumber          srcPid;
    char                data[NAMEDATALEN + NOTIFY_PAYLOAD_MAX_LENGTH];
                                         /* channel name + payload */
} AsyncQueueEntry;
```

So entries are not fixed-size; multiple notifications fit per page until
the page would overflow.

#### Page-number formula

`asyncQueuePageDiff` and `(pageno, offset)` tuples; wraparound handled by
`long_segment_names = false` (small segment-name range, but the queue
is short-lived so this works).

#### Bank-lock partitioning

Same scheme; default `nslots` from `notify_buffers` GUC.

#### Bootstrap path

- `AsyncShmemInit()`: `SimpleLruInit(NotifyCtl, "Notify", NUM_NOTIFY_BUFFERS,
  0, "pg_notify", ..., SYNC_HANDLER_NONE, false)` at `async.c:538`.

#### Recovery path

**Wiped at startup**: `SlruScanDirectory(NotifyCtl, SlruScanDirCbDeleteAll,
NULL)` removes every segment file. Notifications are volatile — they do not
survive a restart.

#### Checkpoint hook

**None**. `pg_notify` is not flushed at checkpoint. The directory is
considered ephemeral.

#### WAL records

**None**. LISTEN/NOTIFY is not WAL-replicated.

#### Truncate policy

`asyncQueueAdvanceTail` removes pages once every backend has read past them.
`SlruDeleteSegment` is used directly (not `SimpleLruTruncate`) because the
"oldest still-needed" cutoff is computed from per-backend cursors rather
than a global xid.

### Serial (SSI tracking)

#### Identity

- **SlruCtl pointer**: `SerialSlruCtl`
- **On-disk directory**: `$PGDATA/pg_serial/`
- **Source**: `src/backend/storage/lmgr/predicate.c`

#### Per-page layout

- **Entry size**: 8 bytes per XID (`SerCommitSeqNo`).
- **Entries per page**: `SERIAL_ENTRIESPERPAGE = BLCKSZ / 8 = 1024`.

#### Page-number formula

`SerialPage(xid) = xid / SERIAL_ENTRIESPERPAGE`.

#### Bank-lock partitioning

`bank_locks[pageno % nbanks]`; default `nslots` from
`serializable_buffers` GUC.

#### Bootstrap path

- `PredicateLockShmemInit`: `SimpleLruInit(SerialSlruCtl, "Serial",
  NUM_SERIAL_BUFFERS, 0, "pg_serial", ..., SYNC_HANDLER_NONE, false)` at
  `predicate.c:814`.

#### Recovery path

`pg_serial` is volatile — predicate-lock state is rebuilt at runtime.

#### Checkpoint hook

`CheckPointPredicate()` flushes the SLRU and the predicate-lock data
structures into a stable form for the checkpointer.

#### WAL records

**None**.

#### Truncate policy

`SerialSetActiveSerXmin` advances the SLRU truncation point as transactions
retire. `SimpleLruTruncate` is invoked with the new cutoff.

### Cross-references

- `[08 SLRU Framework](08_slru_framework.md)` — common SLRU machinery shared with these.
- These SLRUs are *not* covered by the metadata persistence story because
  their data is volatile by design.


---

[Up: index.md](index.md)  |  [Prev: 18 Catalog Inventory](18_catalog_inventory.md)  |  [Next: 20 WAL Record Catalog](20_wal_record_catalog.md)
