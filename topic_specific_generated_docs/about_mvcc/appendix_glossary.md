# Appendix: Glossary

> MVCC Documentation > Appendix > Glossary

---

## A

**Aggressive VACUUM**: A VACUUM mode triggered when a relation's `relfrozenxid` or `relminmxid` is old enough to risk XID wraparound. Scans all pages (including all-visible ones) and freezes all tuples older than `FreezeLimit`. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

**ALL_FROZEN**: A visibility map bit indicating that all tuples on a heap page have frozen xmin values. VACUUM never needs to revisit an all-frozen page. See [Deep Dives: Freeze Map](10_deep_dives.md).

**ALL_VISIBLE**: A visibility map bit indicating that all tuples on a heap page are visible to all current and future transactions. Enables index-only scans to skip heap fetches. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

## C

**CLOG (Commit Log)**: The persistent two-bit-per-transaction status store, on disk as `pg_xact/`. Records whether each transaction is IN_PROGRESS, COMMITTED, ABORTED, or SUB_COMMITTED. See [CLOG and Transaction Status](08_clog_transaction_status.md).

**cmax**: The command ID of the command that deleted a tuple within its transaction. Stored in `t_cid` or encoded as a combo CID. See [Tuple Versioning](04_tuple_versioning.md).

**cmin**: The command ID of the command that inserted a tuple within its transaction. Used by same-transaction visibility checks. See [Visibility Rules](05_visibility_rules.md).

**Combo CID**: A backend-local mechanism that encodes both cmin and cmax in a single `t_cid` field when a tuple is both inserted and deleted within the same transaction. See [Tuple Versioning: ComboCID](04_tuple_versioning.md).

**curcid**: The current command ID in a snapshot. Tuples inserted by the current transaction with `cmin >= curcid` are invisible. See [Snapshot Management](06_snapshot_management.md).

## D

**datfrozenxid**: The oldest unfrozen XID in a database, stored in `pg_database`. CLOG pages for XIDs before this value can be truncated. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

**dead_after**: An output parameter from `HeapTupleSatisfiesVacuumHorizon()` indicating the XID after which a recently-dead tuple became invisible. See [Visibility Rules](05_visibility_rules.md).

**Dense array**: Cache-friendly packed arrays in `ProcGlobal` that mirror selected PGPROC fields (`xids[]`, `subxidStates[]`, `statusFlags[]`), enabling efficient ProcArray scanning. See [Concurrency Infrastructure](07_concurrency_infrastructure.md).

## F

**FreezeLimit**: A VACUUM cutoff XID computed as `nextXID - vacuum_freeze_min_age`. Tuples with xmin older than this can be frozen. In aggressive mode, they must be. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

**Freezing**: The process of replacing a tuple's xmin with `FrozenTransactionId` (by setting `HEAP_XMIN_FROZEN`) so that the tuple is always considered committed regardless of XID wraparound. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

**FrozenTransactionId**: Special XID value 2 that represents an infinitely old committed transaction. Frozen tuples skip snapshot checks entirely. See [Transaction Lifecycle](03_transaction_lifecycle.md).

**FullTransactionId**: A 64-bit transaction identifier containing a 32-bit epoch and 32-bit XID, used internally to avoid wraparound ambiguity. See [Transaction Lifecycle](03_transaction_lifecycle.md).

## G

**GlobalVisState**: Per-backend cached visibility horizon bounds that enable fast dead-tuple-removability checks without taking ProcArrayLock. See [Concurrency Infrastructure](07_concurrency_infrastructure.md).

**Group clearing**: An optimization where multiple backends that need to clear XIDs from ProcArray are batched under a single lock acquisition by a leader backend. See [Concurrency Infrastructure](07_concurrency_infrastructure.md).

## H

**Hint bits**: Flags in `t_infomask` (`HEAP_XMIN_COMMITTED`, `HEAP_XMIN_INVALID`, `HEAP_XMAX_COMMITTED`, `HEAP_XMAX_INVALID`) that cache CLOG lookup results directly in the tuple header. Not WAL-logged. See [Visibility Rules](05_visibility_rules.md).

**HOT (Heap-Only Tuple)**: An optimization where UPDATE creates a new tuple version on the same page without creating new index entries, when no indexed columns change. See [Deep Dives: HOT Chains](10_deep_dives.md).

**HTSV_Result**: The return type from `HeapTupleSatisfiesVacuumHorizon()`: HEAPTUPLE_LIVE, HEAPTUPLE_RECENTLY_DEAD, HEAPTUPLE_DEAD, HEAPTUPLE_INSERT_IN_PROGRESS, HEAPTUPLE_DELETE_IN_PROGRESS. See [Visibility Rules](05_visibility_rules.md).

## I

**Infomask**: The `t_infomask` and `t_infomask2` fields in the tuple header that encode visibility status, lock state, and tuple properties. See [Tuple Versioning](04_tuple_versioning.md).

**Isolation level**: The degree of transaction isolation. PostgreSQL supports READ UNCOMMITTED (treated as READ COMMITTED), READ COMMITTED, REPEATABLE READ, and SERIALIZABLE. See [Snapshot Management](06_snapshot_management.md).

## L

**latestCompletedXid**: A shared memory variable tracking the newest XID that has completed. Used to compute snapshot `xmax` as `latestCompletedXid + 1`. See [Concurrency Infrastructure](07_concurrency_infrastructure.md).

**Lazy VACUUM**: The default VACUUM mode that operates in-place without rewriting the table, using a multi-pass strategy. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

**LP_DEAD**: A line pointer state indicating the item has been declared dead by pruning. Awaiting conversion to LP_UNUSED after index cleanup. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

**LP_UNUSED**: A line pointer state indicating the slot is available for reuse. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

## M

**MultiXactId**: An identifier that represents a set of transactions holding row-level locks on the same tuple. Stored in `t_xmax` with `HEAP_XMAX_IS_MULTI` flag. See [Deep Dives: MultiXact](10_deep_dives.md).

**MVCC (Multi-Version Concurrency Control)**: A concurrency control method that maintains multiple physical versions of each row, enabling readers and writers to operate without blocking each other. See [Executive Summary](01_executive_summary.md).

## O

**OldestXmin**: The oldest XID that any running backend might need, computed from all backends' `PGPROC.xid` and `PGPROC.xmin` plus replication slot minimums. VACUUM cannot remove tuples with xmax >= OldestXmin. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

## P

**PGPROC**: The per-backend shared memory structure containing transaction state (xid, xmin, subxids), used by the ProcArray. See [Concurrency Infrastructure](07_concurrency_infrastructure.md).

**ProcArray**: The global registry of active backends, implemented as dense arrays in `ProcGlobal`. Scanned by `GetSnapshotData()` and `TransactionIdIsInProgress()`. See [Concurrency Infrastructure](07_concurrency_infrastructure.md).

**Pruning**: The process of removing dead tuple versions from HOT chains and reclaiming page space, performed by `heap_page_prune_and_freeze()`. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

## R

**relfrozenxid**: The oldest unfrozen XID in a relation, stored in `pg_class`. All tuples in the relation are guaranteed to have xmin >= relfrozenxid or be frozen. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

**rw-conflict (read-write conflict)**: A dependency where one serializable transaction reads data that another serializable transaction writes. The basis for SSI conflict detection. See [Deep Dives: SSI](10_deep_dives.md).

## S

**SIREAD lock**: A predicate lock used by Serializable Snapshot Isolation (SSI) to track what data serializable transactions have read. Does not block access. See [Deep Dives: SSI](10_deep_dives.md).

**SLRU (Simple Least-Recently-Used)**: A buffer pool mechanism used by CLOG, pg_subtrans, pg_multixact, and pg_commit_ts for managing on-disk page storage. See [CLOG and Transaction Status](08_clog_transaction_status.md).

**Snapshot**: A frozen view of which transactions are in-progress, used for MVCC visibility decisions. Contains xmin, xmax, xip[] boundaries. See [Snapshot Management](06_snapshot_management.md).

**SSI (Serializable Snapshot Isolation)**: PostgreSQL's implementation of the SERIALIZABLE isolation level, which detects dangerous rw-dependency patterns rather than using traditional locking. See [Deep Dives: SSI](10_deep_dives.md).

**SUB_COMMITTED**: An intermediate CLOG status (11) for subtransactions whose parent has not yet been marked COMMITTED. Ensures atomicity across CLOG pages. See [CLOG and Transaction Status](08_clog_transaction_status.md).

**Subtransaction**: A nested transaction created by SAVEPOINT. Receives its own XID and can be independently rolled back. See [Transaction Lifecycle](03_transaction_lifecycle.md).

## T

**t_ctid**: A field in the tuple header that points to the next version of the tuple in an update chain, or to itself if it is the latest version. See [Tuple Versioning](04_tuple_versioning.md).

**t_infomask**: A 16-bit field in the tuple header containing visibility hint bits, lock state, and tuple property flags. See [Tuple Versioning](04_tuple_versioning.md).

**t_xmax**: The transaction ID of the transaction that deleted or updated this tuple version. If invalid (0), the tuple has not been deleted. See [Tuple Versioning](04_tuple_versioning.md).

**t_xmin**: The transaction ID of the transaction that inserted this tuple version. The fundamental MVCC creation stamp. See [Tuple Versioning](04_tuple_versioning.md).

**TidStore**: A radix-tree-based data structure (PostgreSQL 17) used by VACUUM to efficiently store dead item TIDs. Replaces the fixed-size dead_items array. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

**Tuple**: The physical on-disk representation of a row. Each tuple has a header (HeapTupleHeaderData) containing MVCC metadata followed by the row data. See [Tuple Versioning](04_tuple_versioning.md).

## V

**Version chain**: A linked list of tuple versions connected via `t_ctid` pointers, created by successive UPDATEs. See [Tuple Versioning](04_tuple_versioning.md).

**Visibility map (VM)**: A bitmap with two bits per heap page (ALL_VISIBLE and ALL_FROZEN) that enables VACUUM and index-only scans to skip pages. See [VACUUM and Freezing](09_vacuum_and_freezing.md).

**VXID (Virtual Transaction ID)**: A lightweight identifier (`procNumber` + `localTransactionId`) assigned immediately at transaction start. Not stored on disk. See [Transaction Lifecycle](03_transaction_lifecycle.md).

## X

**xactCompletionCount**: A monotonically increasing counter in shared memory, incremented on every transaction completion. Enables snapshot reuse optimization. See [Snapshot Management](06_snapshot_management.md).

**XID (Transaction ID)**: A 32-bit identifier assigned to each writing transaction. Subject to wraparound, hence the need for freezing. See [Transaction Lifecycle](03_transaction_lifecycle.md).

**xip[] (in-progress array)**: The array within a snapshot listing top-level XIDs that were in-progress when the snapshot was taken. XIDs in this range require array search for visibility. See [Snapshot Management](06_snapshot_management.md).

**XID wraparound**: The condition where the 32-bit XID counter exhausts its usable range (~2 billion), potentially causing old committed XIDs to appear "in the future." Prevented by VACUUM freezing. See [Transaction Lifecycle](03_transaction_lifecycle.md).

---

Previous: [Appendix: Symbol Index](appendix_symbol_index.md) | Next: [Appendix: Data Structures](appendix_data_structures.md)
