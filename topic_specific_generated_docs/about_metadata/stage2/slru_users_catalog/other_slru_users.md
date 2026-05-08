# SLRU Users Catalog: Other Users (Notify, Serial)

These two SLRUs are not strictly metadata — they support specific runtime
features — but they share the SLRU framework and merit a brief inventory
entry.

## Notify (LISTEN/NOTIFY)

### Identity

- **SlruCtl pointer**: `NotifyCtl`
- **On-disk directory**: `$PGDATA/pg_notify/`  (note: not under PGDATA's
  global/ or base/, but at PGDATA root)
- **Source**: `src/backend/commands/async.c`

### Per-page layout

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

### Page-number formula

`asyncQueuePageDiff` and `(pageno, offset)` tuples; wraparound handled by
`long_segment_names = false` (small segment-name range, but the queue
is short-lived so this works).

### Bank-lock partitioning

Same scheme; default `nslots` from `notify_buffers` GUC.

### Bootstrap path

- `AsyncShmemInit()`: `SimpleLruInit(NotifyCtl, "Notify", NUM_NOTIFY_BUFFERS,
  0, "pg_notify", ..., SYNC_HANDLER_NONE, false)` at `async.c:538`.

### Recovery path

**Wiped at startup**: `SlruScanDirectory(NotifyCtl, SlruScanDirCbDeleteAll,
NULL)` removes every segment file. Notifications are volatile — they do not
survive a restart.

### Checkpoint hook

**None**. `pg_notify` is not flushed at checkpoint. The directory is
considered ephemeral.

### WAL records

**None**. LISTEN/NOTIFY is not WAL-replicated.

### Truncate policy

`asyncQueueAdvanceTail` removes pages once every backend has read past them.
`SlruDeleteSegment` is used directly (not `SimpleLruTruncate`) because the
"oldest still-needed" cutoff is computed from per-backend cursors rather
than a global xid.

## Serial (SSI tracking)

### Identity

- **SlruCtl pointer**: `SerialSlruCtl`
- **On-disk directory**: `$PGDATA/pg_serial/`
- **Source**: `src/backend/storage/lmgr/predicate.c`

### Per-page layout

- **Entry size**: 8 bytes per XID (`SerCommitSeqNo`).
- **Entries per page**: `SERIAL_ENTRIESPERPAGE = BLCKSZ / 8 = 1024`.

### Page-number formula

`SerialPage(xid) = xid / SERIAL_ENTRIESPERPAGE`.

### Bank-lock partitioning

`bank_locks[pageno % nbanks]`; default `nslots` from
`serializable_buffers` GUC.

### Bootstrap path

- `PredicateLockShmemInit`: `SimpleLruInit(SerialSlruCtl, "Serial",
  NUM_SERIAL_BUFFERS, 0, "pg_serial", ..., SYNC_HANDLER_NONE, false)` at
  `predicate.c:814`.

### Recovery path

`pg_serial` is volatile — predicate-lock state is rebuilt at runtime.

### Checkpoint hook

`CheckPointPredicate()` flushes the SLRU and the predicate-lock data
structures into a stable form for the checkpointer.

### WAL records

**None**.

### Truncate policy

`SerialSetActiveSerXmin` advances the SLRU truncation point as transactions
retire. `SimpleLruTruncate` is invoked with the new cutoff.

## Cross-references

- `component_slru_framework.md` — common SLRU machinery shared with these.
- These SLRUs are *not* covered by the metadata persistence story because
  their data is volatile by design.
