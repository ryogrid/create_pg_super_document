# SLRU Users Catalog: SUBTRANS (pg_subtrans)

## Identity

- **SlruCtl pointer**: `SubTransCtl`
- **On-disk directory**: `$PGDATA/pg_subtrans/`
- **Source**: `src/backend/access/transam/subtrans.c`

## Per-page layout

- **Entry size**: 4 bytes per XID (TransactionId — the parent's XID).
- **Entries per page**: `SUBTRANS_XACTS_PER_PAGE = BLCKSZ / 4 = 2048`.
- **Per-page total**: 2048 (xid → parent_xid) entries.

## Page-number formula

```c
TransactionIdToPage(xid)  = xid / SUBTRANS_XACTS_PER_PAGE
TransactionIdToEntry(xid) = xid % SUBTRANS_XACTS_PER_PAGE
```

## Bank-lock partitioning

Same as other SLRUs: `bank_locks[pageno % nbanks]`. Default `nslots` from
`subtransaction_buffers` GUC.

## Bootstrap path

- `BootStrapSUBTRANS()`: zero page 0.
- `SUBTRANSShmemInit()`: `SimpleLruInit(SubTransCtl, "Subtrans", ...,
  "pg_subtrans", ..., SYNC_HANDLER_NONE, false)` at `subtrans.c:244`.

Note: `SYNC_HANDLER_NONE` — pg_subtrans is **not fsynced** at checkpoint.
Crash safety comes from runtime reconstruction.

## Recovery path

- `StartupSUBTRANS(oldestActiveXID)`: zero every page from
  `TransactionIdToPage(oldestActiveXID)` through the page containing
  `nextXid`. The contents from before the crash are discarded.

## Checkpoint hook

```c
void CheckPointSUBTRANS(void) { SimpleLruWriteAll(SubTransCtl, true); }
```

Flushes dirty pages but does NOT issue sync requests (sync handler is NONE).

## Extend / Truncate

- **Extend**: implicit, when `SubTransSetParent` writes to a page that
  doesn't exist, `SimpleLruZeroPage` allocates the slot. **No WAL is
  emitted** (this is the key SUBTRANS-vs-CLOG difference).
- **Truncate**: `TruncateSUBTRANS(oldestXact)` from `vac_truncate_clog`.
  Drops segments older than the cutoff. **No WAL emitted.**

## WAL records

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

## Wraparound considerations

The same modular `PagePrecedes` callback. Wraparound is handled by
truncation in lockstep with CLOG truncation.

## Retention

Pages older than `vac_truncate_clog`'s cutoff are removed by
`TruncateSUBTRANS`.

## Cross-references

- `component_subtrans.md` — full deep dive.
- `component_slru_framework.md` — SLRU machinery.
