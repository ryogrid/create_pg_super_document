# 21 — Deep Dives

[Up: index.md](index.md)  |  [Prev: 20 WAL Record Catalog](20_wal_record_catalog.md)  |  [Next: appendix_symbol_index.md](appendix_symbol_index.md)

## Prerequisites

- All preceding chapters. This chapter is a collection of essays on
  cross-cutting topics that deserve more attention than the per-component
  chapters can give them.

This chapter contains eighteen focused essays. Use it for reference;
each essay is self-contained.

---

## 1. The relmapper bootstrap-circularity solution

PostgreSQL's relcache cannot read pg_class to find pg_class's own
relfilenode — that would require reading pg_class in order to read
pg_class. The same chicken-and-egg holds for `pg_attribute`,
`pg_proc`, `pg_type`, and every shared catalog (whose `pg_class` row
lives in each database, but whose physical file is one cluster-wide
file).

The cure is the **relmapper**: a small (524-byte typical) side file
listing `(catalog OID → relfilenode)` pairs, written atomically and
replicated via `XLOG_RELMAP_UPDATE`.

- Shared map: `$PGDATA/global/pg_filenode.map` — covers the 11 shared
  catalogs (and their indexes/toast tables).
- Local map: `$PGDATA/base/<dbid>/pg_filenode.map` — covers the 4
  nailed local catalogs (`pg_class`, `pg_attribute`, `pg_proc`,
  `pg_type`) and their indexes.

The relmap is structured as a fixed-size array of 64
`(Oid, RelFileNumber)` entries (`MAX_MAPPINGS = 64`,
`relmapper.c:81`), followed by a CRC. The atomic write protocol is
`WAL → tmp file → fsync tmp → rename → fsync directory`. A crash
anywhere is recoverable: the WAL record alone is enough to rebuild
the file, and `relmap_redo` (`relmapper.c:1096`) re-runs the same
atomic-write protocol.

Because mapped relations cannot be relocated by transactional commands
(the `relfilenode` change requires a relmap update outside the heap
WAL stream), the only operations that move them are VACUUM FULL,
CLUSTER, and REINDEX. Plain `ALTER TABLE ... SET TABLESPACE` is
forbidden for mapped relations (`tablecmds.c::ATPrepSetTableSpace`).

Detailed walkthrough: chapter [07 Relmapper](07_relmapper.md).

---

## 2. The pg_internal.init shortcut and its invalidation

A backend startup that walked pg_class for every nailed and shared
catalog descriptor would spend many milliseconds per start. The
`pg_internal.init` shortcut serializes the relcache entries that
satisfy *both*:

- `rd_isnailed && criticalRelcachesBuilt`,
- the descriptor was built from catalog data (not just `formrdesc`).

There are two such files:

- `$PGDATA/global/pg_internal.init` — for shared catalogs.
- `$PGDATA/base/<dbid>/pg_internal.init` — for per-database catalogs.

`load_relcache_init_file` (relcache.c) deserializes them at
`RelationCacheInitializePhase3`; on validation failure the slow
catalog-scan path is taken.

The interesting part is **invalidation**. Any catalog mutation that
makes the snapshot stale (creating an index on a catalog, ALTERing a
catalog, etc.) calls `RelationCacheInitFileInvalidate`. To avoid an
in-flight DDL leaving the file in a half-applied state, the rename
happens in two phases:

1. *Pre-prepare*: rename `pg_internal.init → pg_internal.init.<pid>`.
   The next backend skips loading and rebuilds.
2. *Post-commit*: `unlink(pg_internal.init.<pid>)`.

If the DDL aborts between the two phases, the file is still in its
original location; backends just rebuild — over-invalidation is safe.

The `RelcacheInitFileInval` flag in `xl_xact_commit` propagates this
invalidation to standbys: `ProcessCommittedInvalidationMessages`
unlinks the standby's local `pg_internal.init` files at the same
point in the redo stream.

---

## 3. CatCache negative entries and their hash-bucket invalidation

When `SearchCatCacheInternal` performs a cold-path catalog scan and
finds *no* matching row, it inserts a "negative" CatCTup with
`ct->negative = true`. The next miss-of-the-same-key returns NULL
quickly without scanning the catalog.

This optimization is essential for hot lookups that legitimately miss
(e.g., `RELNAMENSP` lookups for nonexistent relations during
`get_relname_relid` retries).

The subtlety: when a sinval message arrives that says "this catalog
row may have changed", the corresponding **hash bucket** must be
purged of *both* positive and negative entries with that hash. If a
negative entry survived, the next lookup would falsely answer "no
such row" even though the row was just created.

`CatCacheInvalidate(cache, hashValue)` (`catcache.c:625`) walks every
bucket whose hash matches and marks every matching CatCTup as `dead`,
including negatives. The dead/refcount dance ensures concurrent
holders of the entry see a consistent view (positive entries with
non-zero refcount stay around until released).

---

## 4. sinval queue overflow → SI_RESET correctness argument

The sinval ring buffer holds at most `MAXNUMMESSAGES ≈ 4096 ×
MaxBackends` slots. If a backend falls so far behind that
`SendSharedInvalidMessages` cannot make room without dropping
unread messages, it sets `resetState = true` for the laggard's
`ProcState`. The next `ReceiveSharedInvalidMessages` call sees the
flag, calls `InvalidateSystemCachesExtended`, and wipes **every**
catcache and relcache entry.

**Why is this correct?**

Because **over-invalidation is safe** but **under-invalidation is not**.

- Over-invalidation (SI_RESET): the next access rebuilds from
  pg_catalog. Because catalogs are MVCC-correct, the rebuild produces
  an at-least-as-fresh view. Cost: rebuild time, never staleness.
- Under-invalidation (lost message): a stale CatCache row is returned
  by the next lookup. Subsequent queries see, e.g., "the type's
  pg_type row" with the wrong typoutput, leading to silently wrong
  query results.

The single bit `resetState` therefore turns a queue-overflow event
into a guaranteed-correct (if slow) outcome rather than silent data
corruption. The cost is borne by the laggard alone.

---

## 5. The commit-time invalidation contract — ordering with TransactionIdCommitTree

Why must `ProcessCommittedInvalidationMessages` run **after** the
CLOG bit is set but **before** any other backend can witness the
committed state?

If a standby reader sees CLOG.committed before it processes the
sinval messages, it could:

1. Read pg_class via a stale relcache.
2. See the new (committed) row but with old-format tuple descriptor.
3. Mis-decode the tuple.

The primary's `RecordTransactionCommit` enforces this by ordering:

```
XLogInsert(commit) → XLogFlush → TransactionIdCommitTree → AtEOXact_Inval
```

Standby's `xact_redo_commit` mirrors it:

```
TransactionIdCommitTree → ProcessCommittedInvalidationMessages → smgrDoPendingDeletes
```

Standby processes proceed serially through WAL, so the standby's
`xact_redo_commit` finishes invalidation before the next replayer
reads any new tuple. There is no inter-record race.

The primary side is more delicate: between CLOG-bit and AtEOXact_Inval
a different backend *could* witness the committed XID via a snapshot
and try to read a stale catalog row. PostgreSQL's invariant is that
catalog mutations always run in transactions whose effects are
inseparable from the cache invalidations they emit; the receiver is
not guaranteed to see the invalidations *before* the commit, but
`AcceptInvalidationMessages` is called at every relation-open and
lock-acquisition, so the staleness window is bounded by one such
event.

---

## 6. SLRU bank-lock partitioning and the move from one global lock

Earlier PostgreSQL versions had one `XactSLRULock` guarding every
slot of every SLRU. With many cores, that became a fatal serial
point. The replacement, introduced incrementally over recent
versions, is **bank locking**.

The hash function (`slru.h`):

```c
static inline LWLock *
SimpleLruGetBankLock(SlruCtl ctl, int64 pageno)
{
    int bankno = pageno % ctl->nbanks;
    return &(ctl->shared->bank_locks[bankno].lock);
}
```

Each bank holds at most 16 slots; `nbanks = nslots / 16` (rounded to
power of 2). Two backends accessing different banks proceed
independently. The hash distributes contention even when access is
concentrated on recent pages, because banks rotate as XIDs advance.

Why `pageno % nbanks` and not slot index? Because a page belongs to
its bank (the slot in which it lives is bank-local), so the hash is
stable across slot reassignment. A page never moves cross-bank; cross-
bank reassignment is forbidden in `SlruSelectLRUPage`.

**Group commit on top of bank locking** (next deep dive) further
reduces contention at the live tail of CLOG, where bank skew is most
extreme.

---

## 7. CLOG group commit (TransactionGroupUpdateXidStatus)

When N committers concurrently target the same CLOG page (the typical
case during heavy commit traffic), the bank-lock would serialize them
even though the work is identical bit-flips. Solution: a leader-
follower scheme, `TransactionGroupUpdateXidStatus` (`clog.c:441`).

Algorithm:

1. The first committer to fail the bank-lock fast-path becomes a
   "queued" entry on a per-page wait list.
2. The first committer to *acquire* the bank-lock becomes the
   "leader". It processes its own update plus every queued update for
   the same page in one critical section.
3. The leader signals each follower's condition variable; followers
   wake up and return success without doing any I/O themselves.

Net effect: O(N) bank-lock acquisitions become O(1) for hot pages.
Up to ~32 followers piggyback on one leader; the leader's cost
per-batch is one lock, one page-dirty, N memory stores.

This is the single most important scalability win in clog.c.

---

## 8. Why subtrans is not WAL-logged

Every other SLRU is crash-safe via WAL (CLOG, MultiXact, CommitTs) or
explicitly volatile (Notify, Serial). SUBTRANS sits in a special
position: it stores subtransaction parent links, but it is *not* WAL-
logged.

The reasoning:

1. **Visibility checks during replay rely on `xl_xact_commit`'s
   embedded `subxacts[]` array**, not on pg_subtrans. So replay does
   not need pg_subtrans to make WAL replay correct.
2. **Parent links for in-flight (not-yet-finalized) transactions are
   reconstructed at runtime**: `SubTransSetParent` is called from
   every `AssignTransactionId(parent, child)`, and after replay the
   in-shmem subxids array carries the same information.
3. **The `TransactionXmin` floor in `SubTransGetTopmostTransaction`**
   prevents readers from ever consulting an XID below
   `TransactionXmin`. So even a missing or zeroed pg_subtrans entry
   for an old XID is fine — its top-level XID's CLOG bit is final.

So SUBTRANS is initialized with `SyncRequestHandler =
SYNC_HANDLER_NONE` — it is not even fsynced at checkpoint. On
startup, `StartupSUBTRANS(oldestActiveXID)` zeros every page from
`oldestActiveXID` through `nextXid`, discarding pre-crash state.

This makes SUBTRANS the simplest of all the SLRUs (its source file is
under 450 lines) and the cleanest example of "WAL is not always the
right answer".

---

## 9. VM bit-set LSN-aware invariant

The VM stores two bits per heap page: `ALL_VISIBLE` and `ALL_FROZEN`.
The bit-set protocol must enforce a subtle LSN invariant:

> **The VM page's LSN must not regress below the youngest tuple it
> claims is visible.**

Why: when vacuum decides "page P is all-visible at LSN L", the VM bit
should not be set on a VM-page-image that has LSN < L. Otherwise a
crash followed by recovery could leave the VM saying "all visible"
while the heap page actually has un-visible tuples (because its LSN
was further forward and the VM bit is set).

`visibilitymap_set` (`visibilitymap.c:244`) takes a `recptr`
parameter (the heap-mutation LSN that motivated the VM update) and
sets the VM page's LSN to at least `recptr`:

```c
if (PageGetLSN(vmpage) < recptr)
    PageSetLSN(vmpage, recptr);
```

This means even if the VM page is dirty-without-WAL-of-its-own (the
common case, because the bit-set is logged via `XLOG_HEAP2_VISIBLE`
which writes the VM page's LSN explicitly), the LSN order on disk
preserves the safety invariant.

A corollary: if a torn-page-style recovery rolls a VM page back to
some pre-redo state, the high-water-mark `recptr` ensures any
re-replay that has the same LSN already does not double-set the bit.

---

## 10. XLOG_HEAP2_VISIBLE conditional FPI

The `xl_heap_visible` payload is small (`TransactionId cutoff_xid +
uint8 flags`). Whether the WAL record carries a full-page image of
the VM page depends on:

1. **The standard "first dirty after checkpoint" rule**: if the VM
   page's last LSN < the most recent checkpoint's redo pointer, an
   FPI is required for torn-page protection.
2. **Hint-aware rules**: if `wal_log_hints = on` or data checksums
   are enabled, every VM-page write requires an FPI to protect the
   non-hint data on the page (since checksums fail on a torn write).

In high-throughput workloads, the FPI is emitted roughly once per
checkpoint cycle; subsequent `visibilitymap_set` calls on the same
page within the same cycle log only the bit changes. This keeps the
WAL size bounded.

The heap-side FPI within the same record is similar: setting
`PD_ALL_VISIBLE` on the heap page is a hint, but the heap page's FPI
is already governed by the rules for ordinary heap mutations.

---

## 11. VM bit-clear is implicit in heap WAL

Unlike bit-set, bit-clear is **not** a separate WAL record. Instead,
every heap-mutation record's redo function calls `visibilitymap_clear`:

- `heap_xlog_insert`
- `heap_xlog_update`
- `heap_xlog_delete`
- `heap_xlog_lock`
- `heap_xlog_multi_insert`

This piggyback saves one WAL record per heap mutation.

Why is bit-clear safe to piggyback while bit-set is not? Asymmetry of
errors:

- **Bit-set is a *new* assertion** ("nothing un-visible on this
  page"); it must be durable in its own right, and the corresponding
  `cutoff_xid` must be remembered for recovery.
- **Bit-clear is a *retraction*** ("might be un-visible now");
  over-clearing is safe (just causes a heap fetch), so no special
  record is needed. Even if redo runs twice, the bit ends up clear
  and the heap is consulted — the invariant holds.

This asymmetry is also why the VM stores two bits but tracks them
separately in WAL: the contract for each bit is different.

---

## 12. FSM as a hint, not a record — the recomputability argument

The Free Space Map is the *only* metadata structure that has
essentially **zero WAL traffic** (apart from the implicit
`XLOG_FPI_FOR_HINT` when hint-bit-style protection is on).

Why is this safe?

- **Quality only**: a wrong FSM costs at most one extra page-read or
  a slightly suboptimal insert location. It cannot cause incorrect
  query results.
- **Recomputable**: VACUUM walks the heap and rewrites the FSM from
  scratch (`FreeSpaceMapVacuum`). Any drift between the actual heap
  and the FSM hint is corrected at vacuum time.
- **Tolerant readers**: `RBM_ZERO_ON_ERROR` is used when reading FSM
  pages, so a corrupt page is silently re-zeroed and treated as "no
  free space anywhere on this leaf". The next vacuum repairs.

The contrast with VM is instructive: VM_ALL_VISIBLE is a *correctness*
hint (an index-only scan that trusts a stale ALL_VISIBLE bit returns
wrong rows), while FSM is a *performance* hint (a stale FSM costs one
extra page read).

The one exception, `XLogRecordPageWithFreeSpace`, is for the
heap-extension special case: when the primary creates a new heap
page, the FSM update for that new page is recorded so a standby has
the same hint. Without this, a standby that is read-only could waste
extension churn.

---

## 13. hio.c deadlock-avoidance protocol (pin-VM-before-lock-heap)

When a heap inserter wants to clear the VM bit on a target page (so
inserting into the page does not break the all-visible invariant),
it must access both the VM buffer and the heap buffer. Vacuum, on
its way to setting the VM bit, also wants both.

A naive locking order would deadlock:

```
inserter:    lock heap → pin VM → ...
vacuum:      pin VM → lock heap → ...
```

Inserter A holds heap lock, waits for VM pin. Vacuum holds VM pin,
waits for heap lock. Deadlock.

The fix: **always pin the VM buffer before acquiring an exclusive
lock on the heap buffer**. Both inserter and vacuum follow:

```
1. visibilitymap_pin(rel, target_blk, &vmbuf)   /* pin VM first */
2. LockBuffer(heap_buf, BUFFER_LOCK_EXCLUSIVE)  /* lock heap second */
3. ... do work ...
4. LockBuffer(heap_buf, BUFFER_LOCK_UNLOCK)
5. ReleaseBuffer(vmbuf)
```

Pin acquisition is non-blocking (it just bumps a refcount), so no
deadlock can arise. `GetVisibilityMapPins` (`hio.c:140`) enforces
this for the two-block-candidate insert case (when
`RelationGetBufferForTuple` is choosing between two pages).

This is the single most important inter-fork locking invariant in
PostgreSQL.

---

## 14. Heap hint bits and `wal_log_hints`

Heap tuples carry an `infomask` byte with hint bits like
`HEAP_XMIN_COMMITTED`, `HEAP_XMIN_INVALID`, `HEAP_XMAX_COMMITTED`,
`HEAP_XMAX_INVALID`. These bits are *derivable* from CLOG: setting
them is purely a performance optimization (it lets visibility checks
skip the CLOG lookup).

Because hint bits are derivable, the page write that flips them does
not normally need its own WAL record — `MarkBufferDirtyHint` marks the
page dirty without WAL traffic.

The exception is **torn-page protection under data checksums**. A
non-atomic 8 KB page write that is interrupted by power loss could
corrupt the page checksum, making the page unreadable. To avoid this:

- If `wal_log_hints = on` (a GUC) or `data_checksums = on` (initdb),
  `MarkBufferDirtyHint` emits `XLOG_FPI_FOR_HINT` — a full-page image
  of the page at the moment of the hint-bit write.
- On replay, the FPI is restored, giving a consistent page image
  regardless of whether the hint write completed.

The cost is real: hint-bit writes are very common (every catcache
miss potentially flips one), and `wal_log_hints` can produce
significant WAL volume. The benefit is checksum correctness in the
face of torn writes.

---

## 15. MultiXact wraparound (offsets vs members)

MultiXactId is a 32-bit counter that wraps at 4 billion. Independent
of that, the MultiXact members file (which holds variable-length
member arrays) uses its own 32-bit offset counter that also wraps.

A subtle hazard arises: the *members* counter can wrap before the
*multi* counter does, because long-lived multis with many members
consume disproportionately many member offsets.

PostgreSQL tracks both wraparound horizons:

- `multiVacLimit` / `multiWarnLimit` / `multiStopLimit` for
  MultiXactIds (computed from `oldestMulti`).
- Independent triplet for member offsets, computed in
  `SetOffsetVacuumLimit` (`multixact.c:2705`), based on
  `MultiXactMemberFreezeThreshold`.

`MultiXactMemberFreezeThreshold` approximates "how much member space
is consumed per multi" so vacuum can decide based on the members-file
pressure rather than the multi-id-space pressure. If members are
running out faster, vacuum's freeze threshold is lowered so more
tuples get their xmax replaced with the current top-level XID,
freeing up multis (and hence members).

If members run out before vacuum catches up, `GetNewMultiXactId`
ereports an error: "multixact members exhausted". Operators must run
aggressive vacuum to advance `oldestMulti`. This is one of the rare
errors that can take down a cluster's writability.

---

## 16. catversion.h binding and binary-incompatible catalog changes

`CATALOG_VERSION_NO` (in `src/include/catalog/catversion.h`) is a
monotonic integer that captures the *catalog header layout*. It is
written into `pg_control.catalog_version_no` at initdb time and
checked at every backend start.

The binding between binary and on-disk catalog is therefore:

```
postgres binary built with CATALOG_VERSION_NO = N
   ↓
ReadControlFile() at start
   ↓
if pg_control.catalog_version_no != N:  FATAL
```

This is the mechanism behind "upgrade requires `initdb`": if a
PostgreSQL release adds a column to a catalog, removes a built-in
function, adds a new index, or changes any bki-relevant detail, the
release notes bump `CATALOG_VERSION_NO`. Old `pg_control` files have
the old number; new binaries refuse them.

The only path forward is `pg_upgrade`, which runs the new initdb to
create a fresh `pg_control` and then rewrites the user-data
relfilenodes in place (or by hard-linking) — no row-level
re-marshaling is needed, just the catalog rebuild.

---

## 17. pg_control as the recovery anchor

`pg_control` is the **only** structure a backend can find at startup
without already knowing where everything else is. Its role:

- Cluster identity: `system_identifier` (preventing cross-cluster WAL
  mixing).
- Schema identity: `pg_control_version`, `catalog_version_no`.
- Recovery anchor: `state`, `checkPoint` (LSN of latest checkpoint),
  `checkPointCopy` (inline `CheckPoint` struct with all metadata
  cursors), `minRecoveryPoint`.
- Architecture flags: `maxAlign`, `blcksz`, `xlog_blcksz`,
  `xlog_seg_size`, `nameDataLen`, `indexMaxKeys`,
  `toast_max_chunk_size`, `loblksize`, `float8ByVal`,
  `data_checksum_version`. Using a binary built with different
  values would interpret on-disk pages incorrectly.
- Backup state: `backupStartPoint`, `backupEndPoint`,
  `backupEndRequired`, `backupRecoveryRequired`.
- WAL parameters: `wal_level`, `wal_log_hints`, `MaxConnections`,
  `track_commit_timestamp`. (Some of these are also in the binary's
  GUC defaults, but standby behavior depends on knowing the
  primary's setting.)
- `mock_authentication_nonce` — 32 random bytes used by SCRAM-SHA-256
  to thwart cluster-cross authentication replay.

The file is small (`sizeof(ControlFileData) ≤ 512`) so the write is
atomic on common hardware (sector size = 512 bytes). The file is
padded out to 8 KiB so a wrong-version file produces a
recognizable-format-mismatch error rather than a short-read.

`pg_control` corruption is the worst kind of cluster-level damage.
Recovery options:

1. Restore from backup.
2. `pg_resetwal` — a last-resort tool that synthesizes a new
   pg_control. Will lose any committed transactions whose WAL has not
   been applied.

The CRC validates the file content but cannot reconstruct it.

---

## 18. rmgrlist.h-driven dispatch and custom_rmgr

`src/include/access/rmgrlist.h` is the master list of WAL resource
managers. It is included via `#define PG_RMGR(...)` in two places:

- `xlog.c` to define the `RmgrTable[]` array.
- `xlog_internal.h` to define the `RmgrId` enum.

Each `PG_RMGR(...)` line names a resource manager, gives its
`rm_redo` function, `rm_desc` (for `pg_waldump`), `rm_identify`,
`rm_startup`, `rm_cleanup`, `rm_mask` (for masking tests), and
`rm_decode` (for logical decoding). A WAL record's `rmid` field
selects the row; the record's `info` byte selects the action within
that row's redo function.

The metadata-affecting rmgrs are:

| rmid              | Module                              | Redo function     |
|-------------------|-------------------------------------|-------------------|
| `RM_XLOG_ID`      | xlog.c (checkpoints, NEXTOID, FPI)  | `xlog_redo`       |
| `RM_XACT_ID`      | xact.c                              | `xact_redo`       |
| `RM_SMGR_ID`      | storage.c                           | `smgr_redo`       |
| `RM_CLOG_ID`      | clog.c                              | `clog_redo`       |
| `RM_DBASE_ID`     | dbcommands.c                        | `dbase_redo`      |
| `RM_TBLSPC_ID`    | tablespace.c                        | `tblspc_redo`     |
| `RM_MULTIXACT_ID` | multixact.c                         | `multixact_redo`  |
| `RM_RELMAP_ID`    | relmapper.c                         | `relmap_redo`     |
| `RM_HEAP2_ID`     | heapam.c (XLOG_HEAP2_VISIBLE)       | `heap2_redo`      |
| `RM_COMMIT_TS_ID` | commit_ts.c                         | `commit_ts_redo`  |

**custom_rmgr**: Extensions can register their own RM_* IDs in the
reserved range `RM_EXPERIMENTAL_ID..RM_MAX_ID` (currently 128..255)
via `RegisterCustomRmgr` (`xlog_internal.h`). The custom rmgr
provides its own redo, desc, identify functions. This is rarely used
in practice (a custom AM might use it for AM-specific records) but
the framework is in place.

---

[Up: index.md](index.md)  |  [Prev: 20 WAL Record Catalog](20_wal_record_catalog.md)  |  [Next: appendix_symbol_index.md](appendix_symbol_index.md)
