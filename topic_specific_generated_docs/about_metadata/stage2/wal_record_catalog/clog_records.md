# WAL Record Catalog: CLOG (RM_CLOG_ID)

`RM_CLOG_ID = "CLOG"`, redo function: `clog_redo`
(`src/backend/access/transam/clog.c:1107`).

## XLOG_CLOG_ZEROPAGE  (info 0x00)

- **Header**: `clog.h:55`.
- **Payload**: `int64 pageno` (8 bytes).
- **Emitter**: `ExtendCLOG()` (`clog.c`) — when `GetNewTransactionId`
  advances the next XID onto a new CLOG page.
- **Redo**: `clog_redo` calls `SimpleLruZeroPage(XactCtl, pageno)` then
  `SimpleLruWritePage(XactCtl, slot)` so the standby has the freshly-zeroed
  page on disk before any commit-bit update record references it.
- **Makes durable**: existence of a fresh CLOG page covering 32768 XIDs
  (`CLOG_XACTS_PER_PAGE`).
- **Full-page image**: not applicable (no data page beyond the zeroed
  initial state).
- **Standby effects**: a zero page on disk; no cache invalidations.

## XLOG_CLOG_TRUNCATE  (info 0x10)

- **Header**: `clog.h:56`.
- **Payload**:
  ```c
  /* clog.h:32 */
  typedef struct xl_clog_truncate
  {
      int64         pageno;
      TransactionId oldestXact;
      Oid           oldestXactDb;
  } xl_clog_truncate;
  ```
- **Emitter**: `TruncateCLOG()` (`clog.c`) — called from `vac_truncate_clog`
  after vacuum advances `ShmemVariableCache->oldestClogXid`.
- **Redo**: `clog_redo`:
  1. `AdvanceOldestClogXid(xlrec.oldestXact)` — updates the standby's
     `oldestClogXid`.
  2. `SimpleLruTruncate(XactCtl, xlrec.pageno)` — drops segment files
     before pageno.
- **Makes durable**: the cluster-wide CLOG truncation cutoff.
- **Full-page image**: none.
- **Standby effects**: shrinks pg_xact directory; `oldestXid` cursor
  advances. No cache invalidations.

## Why no XLOG_CLOG_SETSTATUS?

Setting the commit-bit for an XID does not need a CLOG-specific WAL record.
The corresponding `XLOG_XACT_COMMIT` (or `XLOG_XACT_ABORT`) is the durable
truth: its redo function (`xact_redo_commit` / `_abort`) calls
`TransactionIdCommitTree` / `TransactionIdAbortTree` which writes the CLOG
bit. So commit-bit setting is *implicit* in the XACT WAL stream.

The only standalone CLOG WAL records are zero-page (which is needed because
SimpleLruZeroPage is the gateway to a non-existent page) and truncate
(which is a side-effect of vacuum's freeze-horizon advance).

## Cross-references

- `component_clog.md` — full CLOG design.
- `slru_users_catalog/clog.md` — pg_xact directory layout.
- `component_persistence_and_wal_records.md` — XACT records that
  implicitly drive CLOG.
