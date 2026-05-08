# Redo Callbacks: SLRU-touching (`clog_redo`, `multixact_redo`, `commit_ts_redo`)

These three callbacks operate on Simple LRU (SLRU) on-disk
structures. SLRUs are PostgreSQL's pre-WAL-era "page-cached file"
abstraction; recovery records page-zero and truncate operations
explicitly to keep the SLRUs consistent across restart.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `clog_redo` — RM_CLOG_ID = 3

### Identity

* **rmgr id**: `RM_CLOG_ID = 3`
* **rmgr name**: `"CLOG"`
* **redo function**: `clog_redo` at
  `src/backend/access/transam/clog.c:1107`
* **header**: declared in `src/include/access/clog.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `CLOG_ZEROPAGE` | Zero a new clog page |
| `0x10` | `CLOG_TRUNCATE` | Advance `oldestClogXid` + truncate SLRU |

Payload structs:

* `int64 pageno` (CLOG_ZEROPAGE)
* `xl_clog_truncate { int64 pageno; TransactionId oldestXact; Oid oldestXactDb; }` (CLOG_TRUNCATE)

### State mutations

| Target | Action |
|--------|--------|
| `pg_xact/` SLRU | New page allocated and zeroed |
| `pg_xact/` SLRU | Older segments removed via `SimpleLruTruncate` |
| `TransamVariables->oldestClogXid` | Advanced |

### Hot-standby behavior

CLOG records do **not** signal recovery conflicts — visibility
implications come from the per-record commit/abort writes via
`xact_redo_commit/abort`, not from these housekeeping records.

### Idempotency / LSN-skip

* `ZEROPAGE` is idempotent — re-zeroing an already-zero page is a
  no-op.
* `TRUNCATE` is idempotent — truncating to a `oldestClogXid` that's
  already been reached is a no-op.
* Goes through SLRU, not buffer manager — no page-LSN check.

### Crash safety

The SLRU files reflect at least all xids ≤ replayed xid. CLOG slot
writes happen via `xact_redo_commit`/`xact_redo_abort`; these
records ensure the on-disk SLRU framing is kept in sync.

### Example

`CLOG_ZEROPAGE pageno=123`:

1. `slru_zero_page(SimpleLruZeroPage, 123)` — write page 123 of
   `pg_xact/` SLRU as zeros, mark dirty.

`CLOG_TRUNCATE pageno=120 oldestXact=...`:

1. `TransamVariables->oldestClogXid = oldestXact`.
2. `SimpleLruTruncate(pg_xact, 120)` — remove pg_xact segments
   older than page 120.

---

## `multixact_redo` — RM_MULTIXACT_ID = 6

### Identity

* **rmgr id**: `RM_MULTIXACT_ID = 6`
* **rmgr name**: `"MultiXact"`
* **redo function**: `multixact_redo` at
  `src/backend/access/transam/multixact.c:3386`
* **header**: declared in `src/include/access/multixact.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_MULTIXACT_ZERO_OFF_PAGE` | Zero offsets SLRU page |
| `0x10` | `XLOG_MULTIXACT_ZERO_MEM_PAGE` | Zero members SLRU page |
| `0x20` | `XLOG_MULTIXACT_CREATE_ID` | Record new multixact |
| `0x30` | `XLOG_MULTIXACT_TRUNCATE_ID` | Truncate offsets+members SLRUs |

Payload structs:

* `xl_multixact_create { MultiXactId mid; MultiXactOffset moff;
  int32 nmembers; MultiXactMember members[FLEXIBLE_ARRAY_MEMBER]; }`
* `xl_multixact_truncate`

### State mutations

* `pg_multixact/offsets` SLRU
* `pg_multixact/members` SLRU
* `MultiXactState` shmem (`nextMXact`, `nextOffset`,
  `oldestMultiXactId`, `oldestMultiXactDB`)

### Hot-standby behavior

Lock-share visibility of multixact members is rebuilt from these
records on the standby, ensuring `MultiXactIdIsRunning` correctly
returns the per-multixact running set.

### Idempotency / LSN-skip

* All operations are idempotent (zero a page, write members at a
  known offset, advance counters).
* Goes through SLRU.

### Crash safety

After replay, `pg_multixact/{offsets,members}` reflect every
multixact created up to the replayed LSN, so visibility checks
work correctly.

---

## `commit_ts_redo` — RM_COMMIT_TS_ID = 18

### Identity

* **rmgr id**: `RM_COMMIT_TS_ID = 18`
* **rmgr name**: `"CommitTs"`
* **redo function**: `commit_ts_redo` at
  `src/backend/access/transam/commit_ts.c:1023`
* **header**: declared in `src/include/access/commit_ts.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `COMMIT_TS_ZEROPAGE` | Zero a new commit_ts page |
| `0x10` | `COMMIT_TS_TRUNCATE` | Truncate commit_ts SLRU |

Payload structs: same shape as CLOG variants.

### State mutations

* `pg_commit_ts/` SLRU pages.

### Hot-standby behavior

Replicates commit-timestamp visibility. The actual per-xid commit
timestamp is written by `xact_redo_commit` via
`TransactionTreeSetCommitTsData`, gated on
`track_commit_timestamp=on`.

### Idempotency / LSN-skip

* Same as CLOG — idempotent SLRU writes.

### Crash safety

Same as CLOG. The SLRU file framing is kept in sync; commit-ts
data is written by `xact_redo_commit` ensuring per-xid coverage.

---

## Source references

* `src/backend/access/transam/clog.c:1107` — `clog_redo`
* `src/backend/access/transam/multixact.c:3386` — `multixact_redo`
* `src/backend/access/transam/commit_ts.c:1023` — `commit_ts_redo`
* `src/include/access/clog.h` — `CLOG_ZEROPAGE`, `CLOG_TRUNCATE`
* `src/include/access/multixact.h` — `XLOG_MULTIXACT_*`
* `src/include/access/commit_ts.h` — `COMMIT_TS_ZEROPAGE`,
  `COMMIT_TS_TRUNCATE`
