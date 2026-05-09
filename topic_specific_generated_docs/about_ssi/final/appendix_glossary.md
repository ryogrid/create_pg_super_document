# Appendix B: Glossary

**Terminology and concepts specific to PostgreSQL's Serializable Snapshot Isolation.**

---

## A

**Abort**
- Termination of a transaction due to an error or detected serialization violation
- Results in rollback of all changes
- Exception code: `40001` (serialization_failure) triggers application-level retry
- See: Chapter 12 (Error Modes and Retries)

**Active Transaction List (ATL)**
- In-memory list of currently executing and recently completed SERIALIZABLE transactions
- Protected by `SerializableXactHashLock`
- Entries summarized to SLRU when old enough
- Typical size: 100-1000 entries

---

## C

**Coalescing**
- Process of combining multiple fine-grained locks into one coarse-grained lock
- Example: 256 tuple locks on same page → 1 page lock
- Triggered when memory exceeds `max_predicate_locks`
- Trade-off: Reduces memory, increases false positive conflicts
- See: Chapter 05 (Predicate Locking), Deep Dive 3

**Conflict Graph**
- Directed graph where nodes = transactions, edges = read-write conflicts
- RW-conflict edge A→B means: A writes data, B reads it (or A reads, B writes)
- Used by dangerous structure detection algorithm
- See: Chapter 06 (Conflict Graph and Detection)

**CommSeqNo (Commit Sequence Number)**
- Monotonically increasing integer assigned when transaction commits
- Determines commit order even if commits appear reordered
- Used for ordering analysis in dangerous structure detection
- Range: 1 to 2^63-1

---

## D

**Dangerous Structure**
- Pattern of three transactions (Tin, Tpivot, Tout) whose committed order would violate serializability
- Pattern: Tin writes → Tpivot reads (Tin's write) and writes → Tout reads (Tpivot's write), and Tin reads (Tout's write)
- Creates cycle: Tin→Tpivot→Tout→Tin
- Detection prevents this cycle, ensuring serializability
- See: Chapter 06, Deep Dive 1, Case Studies

**Deferrable Transaction**
- Transaction declared with `DEFERRABLE` keyword in SERIALIZABLE isolation
- Waits at start for safe snapshot confirmation before proceeding
- Guaranteed zero serialization failures (after safe snapshot acquired)
- Trade-off: Initial latency vs. guarantee of success
- Example: `BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE DEFERRABLE`
- See: Chapter 04, Chapter 08, Case Study 3

---

## G

**GetSafeSnapshot**
- Algorithm to determine if a read-only transaction's snapshot is "safe"
- Safe snapshot = will never have conflicts with any other transaction
- Implemented in `GetSerializableTransactionSnapshot()` when RO_DEFERRABLE
- Enables zero-overhead read-only transactions
- See: Chapter 04, Deep Dive 2

---

## I

**Isolation Level**
- SQL standard categorization of transaction consistency
- SERIALIZABLE = highest level (used by SSI)
- Four levels: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- See: Chapter 03 (Lifecycle and Entry Points)

---

## L

**Lock Granularity**
- Level of specificity for predicate locks
- Three levels (finest to coarsest):
  - TUPLE: specific row
  - PAGE: all rows in 8KB page
  - RELATION: all rows in table
- Finer granularity = more specific conflicts detected (better for accuracy)
- Coarser granularity = less memory (better for scalability)
- See: Chapter 05 (Predicate Locking)

**Lock Promotion**
- Upgrading from fine-grained to coarse-grained locks during predicate lock coalescing
- Example: Tuple lock → Page lock → Relation lock
- Automatic when memory threshold exceeded
- Decision heuristic considers transaction size and lock distribution
- See: Chapter 05, Deep Dive 3

---

## M

**MVCC (Multi-Version Concurrency Control)**
- PostgreSQL's mechanism for maintaining multiple versions of rows
- Allows readers to see consistent snapshots without blocking writers
- Fundamental to SSI: snapshots provide visibility, predicate locks track conflicts
- Integration point: MVCC visibility + conflict detection
- See: Architecture Overview

---

## P

**Predicate**
- Data range or set of rows affected by a transaction
- In SSI, predicate = the relation/page/tuple being accessed
- Predicate lock = marker that transaction accessed this predicate
- Can be explicit (relation/page/tuple) or implicit (index range)
- See: Chapter 05, Architecture Overview

**Predicate Lock / SIREAD Lock**
- Non-blocking marker indicating "this transaction examined this data"
- Different from regular locks (which block conflicting operations)
- Used retroactively to detect conflicts
- SIREAD = "Serializable Isolation Read" lock
- Stored in shared memory hash tables
- See: Chapter 05 (Predicate Locking)

**PredXactList**
- Active Predicate Transaction List: all SERIALIZABLE transactions currently running
- Hash table: XID → SERIALIZABLEXACT mapping
- Protected by `SerializableXactHashLock`
- Entries transition: ACTIVE → FINISHED → SUMMARIZED (in SLRU)
- See: Chapter 09 (Concurrency and Shared Memory)

---

## R

**RO_SAFE**
- Flag indicating read-only transaction with safe snapshot
- Such transactions guaranteed zero serialization failures
- Commit succeeds immediately without conflict checking
- Performance: ~0% overhead for locking
- See: Chapter 04, Chapter 08, Deep Dive 2

**RO_UNSAFE**
- Flag indicating read-only transaction WITHOUT safe snapshot
- Such transactions may experience serialization failures
- Requires normal conflict detection and checking
- See: Chapter 04, Chapter 08

**Read-Write Conflict**
- Conflict pattern where one transaction reads and another writes
- Direction: WR if first writes, second reads; RW if first reads, second writes
- Represented as directed edges in conflict graph
- Three WR conflicts can form dangerous structure
- See: Chapter 06 (Conflict Graph and Detection)

**Retry Pattern**
- Application-level logic to handle `SQLSTATE 40001` serialization failures
- Transaction is aborted, must be retried from beginning
- Must be idempotent (same operation, same result if retried)
- Typical pattern: catch exception → rollback → backoff → retry
- See: Chapter 12 (Error Modes and Retries), Case Studies

---

## S

**Safe Snapshot**
- Snapshot where read-only transaction is guaranteed no conflicts
- Condition: No active SERIALIZABLE transactions that could write conflicting data
- Enables DEFERRABLE transactions to wait for safety
- Performance benefit: Zero predicate lock overhead once safe
- See: Chapter 04, Chapter 08, Deep Dive 2

**Serialization Failure**
- Event where SSI detects dangerous structure at commit time
- Transaction is aborted with error code `SQLSTATE 40001`
- Application must retry transaction
- Not a deadlock (different mechanism)
- See: Chapter 12 (Error Modes and Retries)

**Serializable Snapshot Isolation (SSI)**
- PostgreSQL's SERIALIZABLE isolation level implementation
- Provides ACID serializability without two-phase locking overhead
- Uses snapshot-based reading + predicate lock tracking + dangerous structure detection
- Introduced in PostgreSQL 9.1
- See: Executive Summary, Architecture Overview

**SERIALIZABLEXACT**
- Core data structure representing a SERIALIZABLE transaction
- Contains: vxid, xid, snapshot info, lock lists, conflict lists
- Stored in shared memory hash table
- Lifetime: created at snapshot acquisition, used during txn, summarized after completion
- Typical size: ~200 bytes
- See: Catalog 14 (Data Structures), Chapter 03

**Shared Memory**
- Persistent memory shared across all PostgreSQL backend processes
- SSI state stored here: SERIALIZABLEXACT, PREDICATELOCK, conflict edges, etc.
- Protected by LWLocks (lightweight locks)
- Allocation and bounds: configured by `max_predicate_locks`, `shared_buffers`, etc.
- See: Chapter 09 (Concurrency and Shared Memory)

**Snapshot**
- Consistent point-in-time view of the database
- Contains: xmin (oldest active XID), xmax (highest assigned XID), xip[] (in-progress XIDs)
- Taken at statement start for SERIALIZABLE transactions
- Used for MVCC visibility AND SSI conflict tracking
- See: Chapter 04 (Snapshot and Registration)

**SLRU (Simplified Least Recently Used)**
- Persistent on-disk structure for storing old transaction summaries
- File: `pg_serial` in data directory
- Stores compressed transaction state for conflict checking across server restarts
- Reduces memory by ~80% vs. keeping full in-memory records
- See: Chapter 09, Appendix C (Source Map)

**Summarization**
- Process of compressing old completed transactions to SLRU
- Reduces memory: 200 bytes → ~40 bytes per transaction
- Triggered periodically (approximately every 1 second)
- Keeps recent transactions in-memory for conflict detection
- See: Chapter 09, Deep Dive 4

---

## T

**Tin** (First Transaction)
- First transaction in dangerous structure pattern
- Writes data that others read
- See: Deep Dive 1 (Dangerous Structure Detection)

**Tpivot** (Pivot Transaction)
- Middle transaction in dangerous structure pattern
- Reads Tin's write, writes something else
- See: Deep Dive 1 (Dangerous Structure Detection)

**Tout** (Third Transaction)
- Third transaction in dangerous structure pattern
- Reads Tpivot's write
- See: Deep Dive 1 (Dangerous Structure Detection)

**Transaction Lifecycle**
- Stages: BEGIN → Snapshot Acquisition → ACTIVE → Commit Validation → COMMIT/ABORT
- SSI intervention points: snapshot acquisition, conflict detection, commit validation
- See: Chapter 03 (Lifecycle and Entry Points)

**Two-Phase Commit (2PC)**
- Protocol for coordinating commits across multiple databases/services
- PostgreSQL prepared transaction support: PREPARE TRANSACTION → COMMIT PREPARED
- SSI support: predicate locks persisted for prepared transactions
- Recovery: locks restored if prepared transaction recovered after crash
- See: Chapter 08 (Subtransactions and 2PC)

---

## V

**Visibility Check**
- MVCC logic determining which row versions are visible to current transaction
- Based on snapshot's xmin, xmax, xip[] arrays
- Integration point: SSI conflict detection triggered during visibility checks
- See: Architecture Overview, Chapter 04

**VirtualTransactionId (vxid)**
- Per-backend transaction ID (not globally unique)
- Format: (backend_pid, sequence_number)
- Used locally within backend process
- Persisted in SERIALIZABLEXACT for lifecycle tracking
- Different from XID (globally unique transaction ID)
- See: Catalog 14 (SERIALIZABLEXACT)

---

## W

**WR-Conflict**
- Abbreviation for "Write-Read conflict" or generic "Write-Read conflict"
- Pattern: One transaction writes, another reads (or vice versa)
- Fundamental to SSI: three WR-conflicts form dangerous structure
- See: Chapter 06 (Conflict Graph and Detection)

---

## X

**XID (Transaction ID)**
- Globally unique transaction identifier
- 32-bit value (wraparound after 2^31 transactions)
- Assigned when transaction first writes or explicitly requests XID
- Stored in row headers for MVCC visibility
- Different from vxid (virtual transaction ID)
- See: Catalog 14 (SERIALIZABLEXACT)

---

## Related Reading

- [Symbol Index](appendix_symbol_index.md) - Comprehensive function/struct listing
- [Source Map](appendix_source_map.md) - File locations and module organization
- Executive Summary - Key concepts overview
- Architecture Overview - System design perspective
