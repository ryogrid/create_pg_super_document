# Transaction Lifecycle

## Overview

The transaction lifecycle subsystem is the foundational layer of PostgreSQL's MVCC implementation. It manages the creation, execution, commitment, and abortion of transactions -- each of which produces a unique Transaction ID (XID) that stamps tuples for visibility determination. The core logic resides in `src/backend/access/transam/xact.c`, with XID allocation in `src/backend/access/transam/varsup.c`.

PostgreSQL uses a **lazy XID assignment** strategy: a transaction does not receive a real XID until it performs its first write operation. Read-only transactions operate with only a virtual transaction ID (VXID), avoiding the overhead of XID allocation and the associated impact on the XID wraparound counter.

## Key Concepts

### Transaction Identifiers

PostgreSQL uses several types of transaction identifiers:

- **TransactionId (32-bit)**: The traditional 32-bit XID stored in tuple headers. Subject to wraparound at 2^32 (~4 billion).
- **FullTransactionId (64-bit)**: An epoch-extended XID introduced to avoid wraparound ambiguity internally. Contains a 32-bit epoch and 32-bit XID.
- **VirtualTransactionId**: A combination of `procNumber` + `localTransactionId` assigned immediately at transaction start. Never stored on disk; used only for lightweight locking and identification.

### Special Transaction IDs

Defined in `src/include/access/transam.h`:

| XID | Name | Meaning |
|-----|------|---------|
| 0 | `InvalidTransactionId` | No transaction |
| 1 | `BootstrapTransactionId` | Used during initdb; always considered committed |
| 2 | `FrozenTransactionId` | Represents a frozen tuple; always visible to all |
| 3+ | `FirstNormalTransactionId` | First normal XID; regular transactions start here |

## Architecture

```mermaid
graph TB
    subgraph "Transaction State Machine"
        DEFAULT["TRANS_DEFAULT<br/>(idle)"]
        START["TRANS_START<br/>(initializing)"]
        INPROG["TRANS_INPROGRESS<br/>(active)"]
        COMMIT["TRANS_COMMIT<br/>(committing)"]
        ABORT["TRANS_ABORT<br/>(aborting)"]
        PREPARE["TRANS_PREPARE<br/>(2PC)"]
    end

    DEFAULT -->|"StartTransaction()"| START
    START -->|"init complete"| INPROG
    INPROG -->|"CommitTransaction()"| COMMIT
    INPROG -->|"AbortTransaction()"| ABORT
    INPROG -->|"PrepareTransaction()"| PREPARE
    COMMIT -->|"CleanupTransaction()"| DEFAULT
    ABORT -->|"CleanupTransaction()"| DEFAULT
    PREPARE -->|"CleanupTransaction()"| DEFAULT
```

See also: `diagrams/transaction_lifecycle.mermaid` for the full state diagram including TBlockState.

## Core APIs

### StartTransaction

#### Purpose

Initializes a new transaction, setting up all per-transaction state, memory contexts, and advertising the backend's virtual transaction ID in the ProcArray.

#### Signature

```c
/* Source: src/backend/access/transam/xact.c:2005-2169 */
static void StartTransaction(void);
```

#### Detailed Description

StartTransaction performs the following steps in order:

1. **State validation**: Asserts the current state is `TRANS_DEFAULT` and the state stack is clean.

2. **State transition**: Sets `s->state = TRANS_START` and clears `fullTransactionId` to `InvalidFullTransactionId`. A real XID is NOT assigned here -- this is the lazy XID assignment principle.

3. **Transaction parameters**: Initializes `nestingLevel`, `gucNestLevel`, clears child XID arrays.

4. **Security context**: Captures current user ID and security context via `GetUserIdAndSecContext()` for later restoration on abort.

5. **Read-only detection**: If `RecoveryInProgress()` returns true (hot standby), marks the transaction as read-only. Otherwise uses the `default_transaction_read_only` GUC.

6. **Isolation level**: Sets `XactIsoLevel` from `DefaultXactIsoLevel` (READ COMMITTED by default).

7. **Command ID reset**: Initializes `currentCommandId` to `FirstCommandId` (0).

8. **Memory contexts**: Calls `AtStart_Memory()` to create `TopTransactionContext` and `CurTransactionContext`.

9. **Resource owner**: Calls `AtStart_ResourceOwner()` to create `TopTransactionResourceOwner`.

10. **Virtual XID assignment**: Allocates a new `LocalTransactionId` and combines it with `MyProcNumber` to form a `VirtualTransactionId`. Inserts a lock table entry and advertises it in `MyProc->vxid.lxid`.

11. **Timestamp**: Sets `xactStartTimestamp` from `stmtStartTimestamp` (for normal transactions) or calls `GetCurrentTimestamp()` (for transactions inside procedures).

12. **Subsystem initialization**: Calls `AtStart_GUC()`, `AtStart_Cache()`, `AfterTriggerBeginXact()`.

13. **State completion**: Sets `s->state = TRANS_INPROGRESS`.

#### Performance Characteristics

StartTransaction is lightweight because it deliberately avoids XID allocation. The most expensive operations are memory context creation and the virtual XID lock table insertion. No shared memory locks are acquired beyond the brief VXID advertisement.

#### Key Invariants

- No real XID is assigned at this point; `fullTransactionId` remains invalid.
- The PGPROC entry is updated with the virtual XID atomically.
- The transaction inherits the session's default isolation level and read-only setting.

---

### GetNewTransactionId

#### Purpose

Allocates the next available XID from the global counter. Called lazily when a transaction first needs a real XID (i.e., on the first write operation). Also handles XID wraparound protection.

#### Signature

```c
/* Source: src/backend/access/transam/varsup.c:76-282 */
FullTransactionId GetNewTransactionId(bool isSubXact);
```

#### Parameters

| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| isSubXact | bool | True if allocating for a subtransaction | Determines whether XID goes into PGPROC.xid or subxids cache |

#### Return Value

Returns a `FullTransactionId` (64-bit) containing the newly allocated XID.

#### Detailed Description

1. **Parallel mode check**: Errors out if called during a parallel operation, since parallel workers synchronize transaction state at operation start.

2. **Bootstrap handling**: During bootstrap, returns `BootstrapTransactionId` (1) directly.

3. **Recovery check**: Errors out if called during recovery (standby cannot assign XIDs).

4. **Lock acquisition**: Acquires `XidGenLock` in exclusive mode to serialize XID allocation.

5. **Read current XID**: Reads `TransamVariables->nextXid` and extracts the 32-bit XID.

6. **Wraparound protection**: If the XID has reached or passed `xidVacLimit`:
   - Signals the postmaster to start autovacuum (every 64K transactions).
   - If past `xidStopLimit`: ERROR -- refuses to assign new XIDs.
   - If past `xidWarnLimit`: WARNING with remaining XID headroom.
   - Releases and re-acquires `XidGenLock` (to avoid holding it during error reporting).

7. **CLOG extension**: Calls `ExtendCLOG(xid)` to ensure the CLOG page for this XID exists. Also extends `pg_subtrans` and `pg_commit_ts`.

8. **Advance counter**: Increments `TransamVariables->nextXid`.

9. **ProcArray advertisement**: Stores the new XID into the backend's shared state BEFORE releasing `XidGenLock`:
   - For top-level transactions: Sets `MyProc->xid` and `ProcGlobal->xids[MyProc->pgxactoff]`.
   - For subtransactions: Adds to `MyProc->subxids.xids[]` if the cache has room (up to `PGPROC_MAX_CACHED_SUBXIDS` = 64), otherwise sets the overflow flag.

10. **Lock release**: Releases `XidGenLock`.

#### Integration Points

- **Called by**: `AssignTransactionId()` in xact.c (when a write operation needs a real XID)
- **Calls**: `ExtendCLOG()`, `ExtendSUBTRANS()`, `ExtendCommitTs()`
- **Shared state**: `TransamVariables->nextXid` (global XID counter), `ProcGlobal->xids[]` (dense ProcArray mirror)

#### Performance Characteristics

- **Lock contention**: `XidGenLock` is exclusive but held briefly. The dense array write is a single store.
- **CLOG extension**: Only performed when crossing a page boundary (~32K XIDs), so almost always a no-op.
- **Memory barrier**: Uses `pg_write_barrier()` for subtransaction XID cache updates to ensure proper ordering visible to concurrent readers.

---

### CommitTransaction

#### Purpose

Performs the complete transaction commit sequence: fires deferred triggers, writes the WAL commit record, updates CLOG, clears ProcArray, releases locks, and cleans up all per-transaction resources.

#### Signature

```c
/* Source: src/backend/access/transam/xact.c:2172-2451 */
static void CommitTransaction(void);
```

#### Detailed Description

The commit path has three distinct phases:

**Phase 1: Pre-commit (user code may still run)**

1. **Deferred triggers**: Fires all pending deferred triggers in a loop with portal cleanup, until no more work remains.
2. **Pre-commit callbacks**: Invokes `XACT_EVENT_PRE_COMMIT` callbacks.
3. **Parallel cleanup**: Calls `AtEOXact_Parallel(true)` to clean up any parallel workers.
4. **Serialization check**: For SERIALIZABLE transactions, calls `PreCommit_CheckForSerializationFailure()` to detect rw-dependency cycles (SSI).

**Phase 2: Durable commit (HOLD_INTERRUPTS)**

5. **State transition**: Sets `s->state = TRANS_COMMIT`.
6. **RecordTransactionCommit()**: The critical durability point:
   - Writes the XLOG commit record via `XactLogCommitRecord()`.
   - For synchronous commit: flushes WAL, then updates CLOG via `TransactionIdCommitTree()`.
   - For asynchronous commit: sets the async commit LSN, updates CLOG with the LSN for later flush.
   - Computes `latestXid` across all children.
7. **ProcArrayEndTransaction()**: Clears `MyProc->xid` from the ProcArray, making the transaction invisible to new snapshots. Advances `latestCompletedXid` and increments `xactCompletionCount`.

**Phase 3: Post-commit cleanup**

8. **Resource release**: Releases buffer pins, relation cache entries, invalidation messages, locks (in a specific order designed to maximize concurrent access).
9. **File deletion**: Performs pending file deletions (dropped tables).
10. **Notifications**: Sends NOTIFY signals.
11. **Backend cleanup**: Resets GUC, SPI, enums, namespaces, files, combo CIDs, hash tables, pgstat, snapshots.
12. **State reset**: Sets `s->state = TRANS_DEFAULT`, clears all transaction state fields.

#### Key Invariants

- WAL commit record is flushed BEFORE CLOG is updated (for synchronous commit).
- CLOG is updated BEFORE ProcArray is cleared (so concurrent TransactionIdDidCommit() sees the committed status before the transaction disappears from the running list).
- Locks are released AFTER catalog invalidation messages are sent.
- `HOLD_INTERRUPTS()` prevents cancel/die during the critical durable-commit section.

#### Caller/Callee Relationships

- **Called by**: `CommitTransactionCommand()` via the transaction block state machine
- **Calls**: `RecordTransactionCommit()`, `ProcArrayEndTransaction()`, `AtEOXact_*()` family
- **Critical ordering**: WAL -> CLOG -> ProcArray -> Locks -> Resources

---

### RecordTransactionCommit

#### Purpose

Records the transaction commit in WAL and CLOG. This is the point of durable commit -- once this function returns, the commit is guaranteed to survive crashes (for synchronous commit).

#### Signature

```c
/* Source: src/backend/access/transam/xact.c:1290-1551 */
static TransactionId RecordTransactionCommit(void);
```

#### Return Value

Returns the latest XID among the transaction and all its subtransactions, or `InvalidTransactionId` if the transaction has no XID.

#### Detailed Description

1. **Gather commit data**: Collects pending file deletions, child XIDs, dropped stats, and invalidation messages.

2. **No-XID fast path**: If the transaction never received an XID:
   - Asserts no pending file deletions or stats drops (those require an XID).
   - If there are invalidation messages, emits a standalone WAL record.
   - If no WAL was written at all, returns immediately.

3. **Critical section entry**: Sets `DELAY_CHKPT_START` on `MyProc->delayChkptFlags` to prevent checkpoints from advancing the REDO pointer past our commit record before CLOG is updated.

4. **WAL commit record**: Calls `XactLogCommitRecord()` to write the commit XLOG record containing the XID, subtransaction XIDs, file deletions, invalidation messages, and timestamps.

5. **Sync vs async decision**:
   - **Synchronous** (if `synchronous_commit > OFF`, or `forceSyncCommit`, or `nrels > 0`):
     - `XLogFlush(XactLastRecEnd)` -- flush WAL to disk.
     - `TransactionIdCommitTree()` -- update CLOG with committed status.
   - **Asynchronous** (if `synchronous_commit = OFF` and no file deletions):
     - `XLogSetAsyncXactLSN()` -- tell WAL writer to flush eventually.
     - `TransactionIdAsyncCommitTree()` -- update CLOG with the commit LSN (for hint bit safety).

6. **Critical section exit**: Clears `DELAY_CHKPT_START`.

7. **Synchronous replication wait**: If applicable, waits for synchronous standbys via `SyncRepWaitForLSN()`.

#### Performance Characteristics

- Synchronous commit involves a WAL flush (fsync), which is the dominant cost.
- Asynchronous commit avoids the fsync but risks losing the last few commits on crash.
- The `DELAY_CHKPT_START` flag is a lightweight mechanism (no lock) to coordinate with the checkpointer.

---

### AbortTransaction

#### Purpose

Performs transaction abort processing: records the abort in WAL/CLOG, clears ProcArray, releases all resources, and cleans up per-transaction state.

#### Signature

```c
/* Source: src/backend/access/transam/xact.c:2745-2939 */
static void AbortTransaction(void);
```

#### Detailed Description

The abort path is designed to be robust in the face of errors. It begins with `HOLD_INTERRUPTS()` and systematically releases resources:

1. **Emergency cleanup**: Releases all LW locks (critical -- we may have longjmp'd out of a locked section), unlocks buffers, resets WAL insertion state, cancels condition variable sleeps.

2. **Lock error cleanup**: Calls `LockErrorCleanup()` to clean up any in-progress lock wait.

3. **State transition**: Sets `s->state = TRANS_ABORT`.

4. **Security reset**: Restores the user ID and security context to their pre-transaction values.

5. **Subsystem abort**: Calls abort handlers for triggers, portals, large objects, notify, relation map, two-phase.

6. **RecordTransactionAbort()**: If the transaction had an XID:
   - Writes a WAL abort record (unless the transaction never wrote any WAL).
   - Updates CLOG to ABORTED via `TransactionIdAbortTree()`.
   - Note: recording an abort is NOT critical for correctness -- an unrecorded abort is equivalent to a crash, and crash-aborted transactions are treated as aborted.

7. **ProcArrayEndTransaction()**: Clears the XID from ProcArray.

8. **Resource cleanup**: Same sequence as CommitTransaction but passing `false` to each cleanup function to indicate abort semantics.

#### Key Difference from Commit

- Abort does NOT enter a critical section or set `DELAY_CHKPT_START`, because losing an abort record is harmless.
- Abort hint bits can always be set safely (unlike commit hint bits which require WAL flush verification).
- The state remains `TRANS_ABORT` until `CleanupTransaction()` resets it to `TRANS_DEFAULT`.

---

### TransactionIdIsCurrentTransactionId

#### Purpose

Determines whether a given XID belongs to the current transaction or any of its subtransactions. Critical for same-transaction visibility checks.

#### Signature

```c
/* Source: src/backend/access/transam/xact.c:925-1006 */
bool TransactionIdIsCurrentTransactionId(TransactionId xid);
```

#### Parameters

| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| xid | TransactionId | The XID to check | Must be a valid, normal XID |

#### Return Value

Returns `true` if the XID is the current top-level transaction or any of its subtransactions (including aborted subtransactions whose effects are still visible to the current transaction).

#### Detailed Description

1. **Top-level check**: Compares against `GetTopTransactionIdIfAny()`.
2. **Subtransaction search**: Walks the transaction state stack checking each level's `fullTransactionId`.
3. **Parallel worker support**: If running as a parallel worker, also searches the `ParallelCurrentXids` array.

## Subtransactions

### Overview

Subtransactions (savepoints) are implemented as nested `TransactionStateData` structures linked via the `parent` pointer. Each subtransaction receives its own XID when it first performs a write.

### PGPROC Subtransaction Cache

Each backend caches up to `PGPROC_MAX_CACHED_SUBXIDS` (64) subtransaction XIDs in its `PGPROC.subxids.xids[]` array. The count and overflow status are tracked in `PGPROC.subxidStatus` and mirrored in `ProcGlobal->subxidStates[]`.

When the cache overflows:
- The `overflowed` flag is set.
- `GetSnapshotData()` marks the snapshot as `suboverflowed`.
- `XidInMVCCSnapshot()` must fall back to `SubTransGetTopmostTransaction()` which consults `pg_subtrans` to resolve subtransaction XIDs to their top-level parent.

### pg_subtrans

The `pg_subtrans` SLRU (`src/backend/access/transam/subtrans.c`) stores a parent XID for each subtransaction XID. It is consulted when the PGPROC subtransaction cache has overflowed. Key functions:

- `SubTransSetParent(mySubid, parentSubid)`: Records the parent relationship.
- `SubTransGetParent(xid)`: Returns the parent XID.
- `SubTransGetTopmostTransaction(xid)`: Recursively walks parents to find the top-level XID.

## Transaction Block States

The `TBlockState` enum (`src/backend/access/transam/xact.c:155-182`) tracks the client-facing transaction block state, which is distinct from the internal `TransState`. Key states:

| State | Meaning |
|-------|---------|
| `TBLOCK_DEFAULT` | Idle, no transaction block |
| `TBLOCK_STARTED` | Running a single implicit transaction |
| `TBLOCK_BEGIN` | Received BEGIN, starting explicit block |
| `TBLOCK_INPROGRESS` | Inside an explicit transaction block |
| `TBLOCK_END` | Received COMMIT |
| `TBLOCK_ABORT` | Error occurred, awaiting ROLLBACK |
| `TBLOCK_SUBBEGIN` | Starting a savepoint |
| `TBLOCK_SUBINPROGRESS` | Inside a savepoint |
| `TBLOCK_SUBABORT` | Savepoint failed |

## XID Wraparound Protection

Since XIDs are 32-bit and wrap around, PostgreSQL must freeze old XIDs before they can be misinterpreted. The wraparound protection thresholds are computed in `SetTransactionIdLimit()` (`src/backend/access/transam/varsup.c:371-503`):

| Threshold | Distance from Wrap | Action |
|-----------|-------------------|--------|
| `xidVacLimit` | `autovacuum_freeze_max_age` from oldest | Start aggressive autovacuum |
| `xidWarnLimit` | 40 million from wrap | Issue WARNING to log |
| `xidStopLimit` | 3 million from wrap | Refuse new XIDs (ERROR) |
| `xidWrapLimit` | Halfway around from oldest | Actual wraparound point |

## Processing Flow

```mermaid
sequenceDiagram
    participant Client
    participant XactMgr as xact.c
    participant VarSup as varsup.c
    participant WAL
    participant CLOG as clog.c
    participant ProcArr as procarray.c

    Client->>XactMgr: BEGIN
    XactMgr->>XactMgr: StartTransaction()
    Note over XactMgr: TRANS_DEFAULT -> TRANS_INPROGRESS
    Note over XactMgr: Assign virtual XID only

    Client->>XactMgr: INSERT (first write)
    XactMgr->>VarSup: GetNewTransactionId(false)
    Note over VarSup: Acquire XidGenLock
    VarSup->>VarSup: Allocate from nextXid
    VarSup->>ProcArr: Store in ProcGlobal->xids[]
    Note over VarSup: Release XidGenLock

    Client->>XactMgr: COMMIT
    XactMgr->>XactMgr: CommitTransaction()
    XactMgr->>WAL: XactLogCommitRecord()
    XactMgr->>WAL: XLogFlush() [sync commit]
    XactMgr->>CLOG: TransactionIdCommitTree()
    XactMgr->>ProcArr: ProcArrayEndTransaction()
    Note over ProcArr: Clear xid, advance latestCompletedXid
    XactMgr->>XactMgr: Release locks and resources
    Note over XactMgr: TRANS_COMMIT -> TRANS_DEFAULT
```

## Implementation Notes

1. **Lazy XID assignment** is critical for performance. Read-only transactions (which are common) never consume an XID, avoiding unnecessary pressure on the XID wraparound counter and reducing ProcArray contention.

2. **The commit critical section** (`DELAY_CHKPT_START`) ensures that if the WAL commit record is written before a checkpoint's REDO point, the corresponding CLOG update will also be flushed before the checkpoint completes. Without this, a crash after the checkpoint but before the CLOG flush could lose the committed status.

3. **ProcArrayEndTransaction ordering**: The XID must be cleared from ProcArray AFTER CLOG is updated. Otherwise, a concurrent `GetSnapshotData()` might not see the transaction as running, but `TransactionIdDidCommit()` would also return false (because CLOG has not been updated yet), creating a window where the transaction appears to have neither committed nor be running -- which would be interpreted as aborted.

4. **Abort is always safe**: Even if the abort WAL record is not written (e.g., due to a crash during abort), the transaction will be treated as aborted because the CLOG status remains `IN_PROGRESS` and the presumption after crash recovery is that unfinished transactions aborted.

## Source File References

| File | Key Symbols | Lines |
|------|-------------|-------|
| `src/backend/access/transam/xact.c` | `StartTransaction`, `CommitTransaction`, `AbortTransaction`, `RecordTransactionCommit` | 2005-2169, 2172-2451, 2745-2939, 1290-1551 |
| `src/backend/access/transam/varsup.c` | `GetNewTransactionId`, `SetTransactionIdLimit` | 76-282, 371-503 |
| `src/include/access/transam.h` | `TransamVariablesData`, special XID constants | -- |
| `src/backend/access/transam/subtrans.c` | `SubTransSetParent`, `SubTransGetTopmostTransaction` | -- |
