# SLRU Users Catalog: CLOG (pg_xact)

## Identity

- **SlruCtl pointer**: `XactCtl` (alias for `&XactCtlData`)
- **On-disk directory**: `$PGDATA/pg_xact/`
- **Source**: `src/backend/access/transam/clog.c`

## Per-page layout

- **Entry size**: 2 bits per XID (XidStatus).
- **Entries per page**: `CLOG_XACTS_PER_PAGE = BLCKSZ * CLOG_XACTS_PER_BYTE
  = 8192 * 4 = 32768`.
- **Total per page**: 32768 XIDs in 8 KiB.

## Page-number formula

```c
TransactionIdToPage(xid) = xid / CLOG_XACTS_PER_PAGE
```

Within a page:
- byte index = `(xid % CLOG_XACTS_PER_PAGE) / CLOG_XACTS_PER_BYTE`
- bit-pair  = `(xid % CLOG_XACTS_PER_BYTE) * 2`

## Bank-lock partitioning

`SimpleLruGetBankLock(XactCtl, pageno) = bank_locks[pageno % nbanks]` where
`nbanks = nslots / 16`. Default `nslots` from `xact_buffers` GUC (-1 means
auto-tune).

## Bootstrap path

- `BootStrapCLOG()`: zero page 0, write it, fsync.
- `CLOGShmemInit()`: `SimpleLruInit(XactCtl, "Xact", nslots,
  CLOG_LSNS_PER_PAGE, "pg_xact", ..., SYNC_HANDLER_CLOG, false)` at
  `clog.c:811`.

## Recovery path

- `StartupCLOG()`: set `latest_page_number = TransactionIdToPage(nextXid - 1)`.
- `TrimCLOG()`: zero the trailing portion of the live page beyond `nextXid`.

## Checkpoint hook

```c
void CheckPointCLOG(void) { SimpleLruWriteAll(XactCtl, true); }
```

Flush every dirty page; sync requests are queued for the checkpointer.

## Extend / Truncate

- **Extend**: `ExtendCLOG(newestXact)` from `GetNewTransactionId`. Emits
  `XLOG_CLOG_ZEROPAGE` and `SimpleLruZeroPage` when the new XID falls on
  a fresh page.
- **Truncate**: `TruncateCLOG(oldestXact, oldestxid_datoid)` from
  `vac_truncate_clog`. Emits `XLOG_CLOG_TRUNCATE` (xl_clog_truncate
  payload), advances `ShmemVariableCache->oldestClogXid`, calls
  `SimpleLruTruncate`.

## WAL records

| Record name           | info | redo function | payload          |
|-----------------------|------|---------------|------------------|
| XLOG_CLOG_ZEROPAGE    | 0x00 | clog_redo     | int64 pageno     |
| XLOG_CLOG_TRUNCATE    | 0x10 | clog_redo     | xl_clog_truncate |

`clog_redo` lives at `src/backend/access/transam/clog.c:1107`.

## Wraparound considerations

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

## Retention

CLOG entries are kept until cluster-wide `oldestXid` (= `min(datfrozenxid)`
across pg_database) advances past the page. Vacuum's freezing logic drives
this advance.

## Group commit (TransactionGroupUpdateXidStatus)

When N committers concurrently target the same CLOG page, `TransactionIdSetPageStatus`
falls back to a per-page leader-follower queue: one leader acquires the
bank-lock, processes its own + all queued requests in one critical section,
and signals the followers. Converts O(N) lock acquisitions into O(1) for
hot pages. Defined in `clog.c::TransactionGroupUpdateXidStatus`.

## Async commit and group_lsn

`SimpleLruInit` is called with `nlsns = CLOG_LSNS_PER_PAGE = 1024` (the only
SLRU with a non-zero `nlsns`). Each page has 1024 LSN slots, one per group
of 32 consecutive XIDs. `TransactionIdSetTreeStatus` propagates the commit
LSN into the appropriate slot. `SimpleLruWritePage` then calls
`XLogFlush(max_lsn_in_slot_range)` before the page write.

## Cross-references

- `component_clog.md` — full deep dive.
- `component_slru_framework.md` — SLRU machinery.
- `wal_record_catalog/clog_records.md` — WAL record details.
