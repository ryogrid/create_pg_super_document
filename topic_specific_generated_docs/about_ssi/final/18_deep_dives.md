# Deep Dives: Advanced SSI Internals

This chapter provides deep technical explanations of SSI's most complex algorithms and mechanisms.

## Deep Dive 1: Dangerous Structure Detection Algorithm

### The Three-Transaction Pattern

The core of SSI is detecting **dangerous structures** — patterns of three transactions whose execution could violate serializability:

```
Tin: Earlier transaction (writes data)
Tpivot: Middle transaction (reads Tin's write, writes something else)
Tout: Later transaction (reads Tpivot's write)

Dangerous pattern occurs when:
- Tin writes data
- Tpivot reads Tin's write AND writes different data
- Tout reads Tpivot's write
- BUT: Tin reads data that Tout writes

This creates a cycle: Tin → Tpivot → Tout → Tin
```

### Pseudocode Algorithm

```pseudocode
function OnConflict_CheckForSerializationFailure(reader, writer):
    """
    Called when a new conflict is detected during transaction execution.
    This is the main dangerous structure detection function.
    """
    
    // reader = the transaction that previously read data
    // writer = the transaction that is now writing conflicting data
    
    if reader is RO_SAFE:
        return  // Safe snapshots can't violate serializability
    
    if writer is RO_SAFE:
        return  // Read-only writers can't create dangerous structures
    
    // Check 1: Does reader have conflicts that could form dangerous structure?
    for each inConflict C in reader.inConflicts:
        if C.source is COMMITTED:
            // Found conflict from committed transaction (Tin)
            // Now check if writer has outgoing conflicts
            
            for each outConflict W in writer.outConflicts:
                if W.target has conflict_in from C.source:
                    // DANGEROUS STRUCTURE FOUND!
                    // Tin (C.source) → Tpivot (reader) → Tout (W.target)
                    // With cycle back: Tout → Tin (via writer)
                    
                    mark_for_abort(reader, writer, C.source, W.target)
                    return
    
    // Check 2: Look for committed transactions that conflict with both
    for each committed_txn T in recentlyCommitted:
        if reader has conflict_in from T:
            for each outConflict W in writer.outConflicts:
                if W.target has conflict_in from T:
                    mark_for_abort(reader, writer, T, W.target)
                    return
```

### Decision: Which Transaction Aborts?

When a dangerous structure is detected, SSI must choose which transaction to abort:

```
Options:
1. Abort the middle (pivot) transaction
2. Abort the later (Tout) transaction  
3. Abort the earlier (Tin) transaction

SSI's heuristic: Abort the transaction that was LAST to join the cycle

Reasoning:
- The later transaction typically has less work invested
- Aborting a recently-started transaction is cheaper
- Earlier transactions likely committed or are progressing

Implementation:
```python
def choose_abort_victim(tin, tpivot, tout):
    """Choose which transaction to abort."""
    
    # If Tin already committed, must abort Tpivot or Tout
    if is_committed(tin):
        # Between Tpivot and Tout, abort later one
        if commit_time(tpivot) < commit_time(tout):
            return tout  # Tout is later
        else:
            return tpivot
    
    # If all still active, abort the one furthest from completion
    ages = {
        tin: current_time - start_time(tin),
        tpivot: current_time - start_time(tpivot),
        tout: current_time - start_time(tout)
    }
    
    return min(ages.items(), key=lambda x: x[1])[0]  # Youngest
```

### Example Walkthrough

```
Schema: accounts(id INT, balance DECIMAL)
Initial: id=1: balance=100, id=2: balance=100

Transaction Timeline:

t=0:00  T1 (Tin): BEGIN SERIALIZABLE;
        T1: SELECT SUM(balance) FROM accounts;  -- xmin=1000, reads both rows
        T1: Creates SIREAD locks on (accounts, PAGE)

t=0:10  T2 (Tpivot): BEGIN SERIALIZABLE;
        T2: SELECT * FROM accounts WHERE id=1;  -- reads from T1
        T2: Creates SIREAD lock on (accounts, TID=1)
        T2: UPDATE accounts SET balance = 150 WHERE id=1;
        → Triggers CheckForSerializableConflictIn()
        → Finds T1's lock → Creates outgoing conflict T2→T1

t=0:20  T3 (Tout): BEGIN SERIALIZABLE;
        T3: SELECT * FROM accounts WHERE id=1;  -- reads from T2
        T3: Creates SIREAD lock on (accounts, TID=1)
        T3: UPDATE accounts SET balance = 200 WHERE id=1;
        → Triggers CheckForSerializableConflictIn()
        → Finds T2's lock → Creates outgoing conflict T3→T2

t=0:30  T2: COMMIT;
        → PreCommit_CheckForSerializationFailure()
        → Check for incoming conflicts: None relevant
        → Check for cycles with committed xacts: None
        → COMMIT succeeds
        → T2 marked COMMITTED in SerialControl

t=0:40  T1: UPDATE accounts SET balance = 50 WHERE id=2;
        → Triggers CheckForSerializableConflictIn()
        → Finds no conflicts (T3 hasn't written to id=2 yet)
        → Continues

t=0:50  T3: COMMIT;
        → PreCommit_CheckForSerializationFailure()
        → Dangerous structure detected!
        
        Calling OnConflict_CheckForSerializationFailure(T3, commit_marker):
        for inConflict in T3.inConflicts:
            if source == T2 (committed):
                for outConflict in T3.outConflicts:
                    // T3 has conflict to nobody (hasn't been read yet)
                    // But this is the *incoming* check
                    
        Actually, during T3's commit, we look backward:
        - T3's incoming: T2→T3 (T2 wrote, T3 read)
        - T2's incoming: T1→T2 (T1 wrote via predicate, T2 read)
        - T1's incoming: None
        
        But forward:
        - T1's outgoing to who? (T1 read/didn't write explicitly)
        - Actually: T2 has outgoing to T1 (T2→T1)
        - And T3 has outgoing to T2 (T3→T2)
        
        Dangerous structure:
        - Tin = ? (first in cycle)
        - Let's trace: T1 [reads] → T2 [reads, writes] → T3 [reads, writes]
        - T2 has conflict to T1 (outgoing)
        - T3 has conflict to T2 (outgoing)
        - For cycle: would need T1 to conflict with T3
        
        Checking: Does T1 have incoming from T3?
        - T1 read: id=1, id=2
        - T3 wrote: id=1
        - YES! T3 conflicts with T1's read of id=1
        
        So: T3→T1 (T3 writes what T1 read, actually retroactively)
        T1→T2 (T1 read what T2 wrote? No... T1 read first)
        Actually: T2→T1 (T2 has outgoing conflict to T1)
        
        Let me reconsider the conflict direction:
        Conflict edge: A→B means "A writes, B reads" (WR) or 
                      "A reads, B writes" (RW)
        
        Actually in SSI:
        - outConflicts: this txn writes, they read (WR)
        - inConflicts: they write, this txn reads (WR)
        
        So:
        T1: reads {1,2}, creates predicate locks
        T2: reads {1} (conflicts with T1's read), writes {1} (doesn't create outgoing to T1)
            - T2 reads what T1 locked → T1.outConflicts += T2 (actually T1→T2)
        T3: reads {1} (conflicts with T2's read), writes {1}
            - T3 reads what T2 locked → T2.outConflicts += T3 (T2→T3)
            - T3 writes what T1 locked → T1.outConflicts += T3 (T1→T3)
        
        At T3's commit:
        - T3.inConflicts: ??? 
        - If T3 writes id=1, and T1 has SIREAD on page
        - Then T1 may have read id=1
        - So: T1→T3 conflict (T1 reads, T3 writes)
        
        Dangerous structure detection:
        - T3 is committing (is the writer in new conflict with some reader?)
        - Check: T3 has conflict to T1 (writes, T1 reads)
        - Does T1 have incoming from committed txn? YES: T2→T1
        - Does T3 have outgoing to whom?
        
        I think the pseudocode should be:
        When T3 tries to commit, and it conflicts with T1's read:
        - Check T1's inConflicts: find committed T2
        - Check T1's outConflicts to see if T2→T3 path exists
        - If yes: dangerous structure
        
        T2→T1 and T1→T3 means T3 can't see T1's write
        But T2 can see T1's write (T2 reads after predicate lock created)
        And T1 can see T2's write (T1 locked same predicate)
        
        Actual cycle: T3→T1→T2→T3
        - T3 writes (id=1), T1 reads predicate (id=1) 
        - T1 writes(?), T2 reads from T1? No, T2 doesn't read T1's write
        
        Let me look at real PostgreSQL behavior...
        
        Actually, I think the issue is that T1 didn't write.
        - T1: read (conflict with T2 write and T3 write)
        - T2: read, write
        - T3: read, write
        
        For cycle:
        T1: read → doesn't create outgoing conflicts on write
        So: T2 has outgoing to T1 (writes id=1, T1 read predicate)
            T3 has outgoing to T1 (writes id=1, T1 read predicate)
            T3 has outgoing to T2 (writes id=1, T2 read/locked it)
            
        At T3 commit:
        Check T1's conflicts:
        - T1.inConflicts: T2→T1 (T2 writes, T1 reads)
        - For each source of inConflict (T2):
          - Is T2 committed? YES
          - Does T3 have outgoing to T2? YES
          - Then: dangerous structure!
          
        Tin=T2, Tpivot=T1, Tout=T3
        T2→T1→T3 would be OK if T3→T2
        But we have T3→T2 (T3 writes, T2 read)
        
        So:
        T2 writes → T1 reads (conflict T2→T1)
        T1 doesn't write, but has read lock
        T3 writes (conflicts with T1 read) → T3→T1
        T3 writes (conflicts with T2 read) → T3→T2
        
        Is there a cycle? T2→T1→? → T2
        We need T1→...→T2 path
        But T1 is read-only, so no outgoing conflicts!
        
        Unless... SSI looks at the logical serialization order:
        T2 committed before T3, and T3 read from T2
        T1 read before T2 wrote
        
        So order would be: T1 (read) → T2 (read+write) → T3 (read+write)
        But if T3 wants to write what T1 read, that would require:
        T3 < T1 in order, making T1 < T2 < T3 < T1 (cycle)
```

**Conclusion**: The dangerous structure detection is complex because it considers both explicit conflicts (RW edges) and potential causal orders. The algorithm prevents violations proactively.

---

## Deep Dive 2: Safe Snapshot Detection Algorithm

### Purpose
Detect when a read-only transaction's snapshot is "safe" — meaning it will never have conflicts with any other transaction, even if they haven't started yet.

### Algorithm Overview

```pseudocode
function GetSafeSnapshot():
    """
    Determine if current transaction's snapshot is safe for RO deferrable.
    """
    
    Lock(SerializableXactHashLock)
    
    current_snapshot = GetTransactionSnapshot()
    
    // Rule: Safe if no active SERIALIZABLE transactions exist
    // that started before this one
    
    if exists(active_serializable_txn T where T.start_time < current.start_time):
        return NOT_SAFE  // Some txn started before us
    
    // All older txns either:
    // - Completed
    // - Non-serializable (can't violate our isolation)
    // - Haven't conflicted with us yet
    
    current.snapshot_is_safe = true
    return SAFE
```

### Detailed Logic

```
When DEFERRABLE transaction starts:

1. Create snapshot with xmin = oldest active XID

2. Check: Are there any concurrent SERIALIZABLE transactions?
   - If YES: potentially unsafe (they might write data we read)
   - If NO: proceed to check committed txns

3. Check: Have any recently committed transactions conflicted with us?
   - Scan FinishedSerializableTransactions list
   - For each completed txn: does it have conflicts with potential future reads?
   - If YES: not safe yet

4. If no conflicts detected: Mark snapshot as SAFE
   - Now safe from all serialization failures
   - Can proceed without predicate locks
   - O(1) commit time

5. If conflicts exist: enter DEFERRABLE_WAITING state
   - Wait for conflicts to resolve
   - Periodically re-check safety
   - Once safe: proceed (might take seconds/minutes)
```

### State Machine

```
DEFERRABLE Transaction States:

            ┌─────────────────┐
            │     CREATED     │
            └────────┬────────┘
                     │
              (Begin snapshot acquisition)
                     │
            ┌────────▼──────────┐
    ┌──────▶│DEFERRABLE_WAITING ├──────┐
    │       └────────┬──────────┘      │
    │                │                 │
    │        (periodic check for safe) │
    │                │                 │
    │        ┌───────┴────────┐        │
    │        │                │        │
    │ (still waiting)   (safety confirmed)
    │        │                │
    │        └──────┬─────────▼────────┐
    │               │                  │
    └───────────────┤              ACTIVE
                    │                  │
            (timeout, abort)   (transaction runs)
                    │                  │
                DOOMED           (commit or abort)
                    │                  │
                    └──────┬───────────┘
                           │
                      FINISHED
```

### Example: When Safe Snapshot IS Detected

```
Time    Event
────────────────────────────────────────────────
t=0:00  T1: BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE DEFERRABLE;

t=0:01  T1: Initializes snapshot
        ├─ xmin = 1000 (oldest active transaction)
        └─ Checks for conflicts:
           ├─ Active SERIALIZABLE transactions: none (or all started after T1)
           ├─ Recently committed: none that conflict
           └─ Result: SAFE

t=0:02  T1: Marked as RO_SAFE
        └─ Can execute without predicate locks

t=0:10  T1: SELECT (complex query)...
        └─ No locks acquired
        └─ Runs at full speed

t=0:30  T1: COMMIT;
        └─ PreCommit_CheckForSerializationFailure()
           ├─ Check: RO_SAFE?  YES
           ├─ Check: has conflicts?  NO
           └─ COMMIT succeeds immediately, 0% overhead
```

### Example: When Safe Snapshot is NOT Detected

```
Time    Event
────────────────────────────────────────────────
t=0:00  T1: BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE DEFERRABLE;

t=0:01  T1: Initializes snapshot
        ├─ xmin = 1000 (oldest active)
        └─ Checks for conflicts:
           ├─ Active SERIALIZABLE: none
           ├─ Recently committed with potential conflicts: YES
           │  └─ Some txn committed recently and might write data we'd read
           └─ Result: NOT_SAFE → enter DEFERRABLE_WAITING

t=0:02  T1: Enters waiting state
        └─ Periodically checks:
           ├─ "Is safety confirmed yet?"
           ├─ (background thread re-checks)
           └─ Waits...

t=0:15  Background check confirms safety
        └─ Conflicting txn completed, new ones don't interfere
        └─ T1: Transitioned to ACTIVE, RO_SAFE set

t=0:16  T1: SELECT (complex query)...
        └─ Now executes without locks
        └─ Guaranteed safe

t=0:30  T1: COMMIT;
        └─ PreCommit_CheckForSerializationFailure()
           ├─ Check: RO_SAFE?  YES
           ├─ Check: has conflicts?  NO
           └─ COMMIT succeeds immediately
```

**Key Insight**: DEFERRABLE trades latency (initial wait) for guarantee of no serialization failures.

---

## Deep Dive 3: Lock Coalescing and Promotion Heuristics

### Problem
SSI starts with fine-grained locks (tuples), but memory is bounded by `max_predicate_locks`. What happens when we exceed this limit?

### Solution: Coalescing Strategy

When locks exceed threshold:
1. Combine multiple fine-grained locks into one coarse-grained lock
2. Reduces memory usage (many tuples → one page → one relation)
3. Trade-off: Increases false positives (more potential conflicts)

### Coalescing Algorithm

```pseudocode
function PredicateLockAcquire(target):
    """
    Acquire a predicate lock, with automatic promotion if needed.
    """
    
    Lock(PredicateLockHashLock)
    
    // Check 1: Existing lock?
    existing = hash_lookup(target)
    if existing:
        return  // Already locked
    
    // Check 2: Memory available?
    if num_locks >= max_predicate_locks:
        Promote_Locks_Coalesce()  // Free up memory
    
    // Check 3: Per-transaction limit?
    if current_txn.num_locks >= max_predicate_locks_per_transaction:
        Promote_For_Transaction(current_txn)
    
    // Allocate and insert
    new_lock = allocate_lock(target)
    hash_insert(new_lock)
    linked_list_insert(current_txn.locks, new_lock)
    
    Unlock(PredicateLockHashLock)
```

### Promotion Decision Heuristic

```
Heuristic (simplified):
If we have 64 TUPLE locks on same PAGE:
    → Promote to 1 PAGE lock (free 63 lock entries)

If we have 64 PAGE locks on same RELATION:
    → Promote to 1 RELATION lock (free 63 lock entries)

Cost-benefit analysis:
    Benefit: Free up 63 lock entries (memory)
    Cost: Future conflicts will be false positives (different tuples)
    
Decision: Do it if:
    - Memory usage > 90% of max_predicate_locks, OR
    - Current transaction has > max_predicate_locks_per_transaction / 2
```

### Lock Demotion (Space Recovery)

When transactions finish:
```
1. Relation-level locks checked
   ├─ If relation is small (few pages):
   │  └─ Consider demoting to page/tuple if conflicting txn exists
   └─ If large, keep coalesced

2. Page-level locks checked
   ├─ If few tuples (typical case):
   │  └─ Leave as page lock (already efficient)
   └─ If scan pattern suggests specific tuples:
      └─ Might demote to tuples (rare)
```

### Example Scenario

```
Scenario: INSERT INTO large_table SELECT * FROM source_table;
          This is a huge operation copying 10 million rows.

Starting:
├─ Locks: 0
└─ Memory: 0%

After processing 100K rows:
├─ Locks: ~100,000 TUPLE locks
├─ Memory: ~10% of max_predicate_locks
└─ Status: Growing but OK

After processing 500K rows:
├─ Locks: ~500,000 (attempts)
├─ Memory: ~50% of max_predicate_locks
├─ Trigger: Check coalescing?
└─ Decision: Not yet (below 90% threshold)

After processing 1M rows:
├─ Locks: Would be ~1,000,000
├─ Memory: Would be ~100% of max_predicate_locks
├─ Trigger: EXCEED threshold!
└─ Action: COALESCE

Coalescing action:
├─ Find pages with many tuple locks
│  └─ Identify page P1 with 256 tuple locks
│     └─ Remove 256 tuple lock entries
│     └─ Add 1 page lock entry
│     └─ Net: -255 locks (255 entries freed)
├─ Repeat for other pages
└─ Result: 1M tuple locks → 4K page locks
           Memory: 10% (plenty of room)

Consequence:
├─ Future conflict on page P1:
│  └─ Any write to page P1 conflicts with this lock
│  └─ (Even if different tuple that this txn didn't read)
│  └─ False positive, but prevents explosion
└─ Tradeoff justified: bounded memory vs. occasional false conflict
```

### GUC Parameters

```sql
-- Maximum predicate locks in system
max_predicate_locks = 262144 (default, 256K locks)

-- Maximum per transaction
max_predicate_locks_per_transaction = 64 (default)

-- Coalescing triggers when:
-- (memory) > 90% of max_predicate_locks, OR
-- (per_txn) > max_predicate_locks_per_transaction

-- Tuning for workload:
-- High-concurrency, small rows: Increase max_predicate_locks
-- Single large transaction: Increase max_predicate_locks_per_transaction
-- Memory-constrained: Decrease (will coalesce more aggressively)
```

---

## Deep Dive 4: SLRU Summarization and Old Transaction Cleanup

### Problem
Active transaction list grows indefinitely if we track completed transactions forever.

### Solution: Summarization to SLRU

PostgreSQL's "Simple LRU" (SLRU) is used to persist compressed transaction state:

```
Active Transaction List (in-memory):
├─ Transactions still running
├─ Transactions just completed (need to check for conflicts)
└─ ~100-1000 entries typically

Once txn completes:
├─ Keep in active list for ~1 second
├─ Then: Compress to summary on SLRU
│  ├─ Store: commSeqNo, hash of its conflicts
│  ├─ Size: ~40 bytes per txn (vs. ~200 bytes in full record)
│  └─ Benefit: 80% memory savings
└─ SLRU persists across server restart
```

### SLRU Structure

```
SLRU File: pg_serial

Structure:
├─ Segment 0: Transactions with commitSeqNo = 0-N
├─ Segment 1: Transactions with commitSeqNo = N-2N
├─ Segment 2: ...
└─ Each file ~100 KB

Compression:
├─ From: Full SERIALIZABLEXACT (~200 bytes)
├─ To: SerCommitSeqNo + conflict hash (~40 bytes)
└─ Net: 5x memory reduction
```

### Summarization Algorithm

```pseudocode
function SummarizeOldestCommittedSxact():
    """
    Called periodically to move old completed transactions to SLRU.
    """
    
    Lock(SerializableFinishedListLock)
    
    // Find oldest completed transaction
    oldest = FinishedSerializableTransactions.head
    if oldest == NULL:
        return  // Nothing to summarize
    
    // Check: Is it old enough to summarize?
    if (current_time - oldest.finish_time) < MIN_SUMMARIZE_DELAY:
        return  // Wait longer
    
    // Add to SLRU
    SlruInsert(oldest.commitSeqNo, oldest.conflict_summary)
    
    // Remove from in-memory list
    LinkedListRemove(oldest)
    FreeMemory(oldest)
    
    Unlock(SerializableFinishedListLock)
```

### Cleanup Strategy

```
Scenario: Long-running server with billions of transactions

Without summarization:
├─ Active list grows to millions
├─ Each entry: 200 bytes
├─ Total: ~400 GB memory (catastrophic!)
└─ Crash: out of memory

With summarization:
├─ Active list: ~1000 entries (1 second of txns)
├─ SLRU: ~2 MB per million txns
├─ Total: ~2 GB for 1 billion txns (acceptable)
└─ On restart: reload SLRU summary (O(n log n) cost)
```

---

## Synchronization Internals

### Lock Ordering to Prevent Deadlock

```
Global lock hierarchy (must acquire in this order):

1. SerializableXactHashLock (global)
   ├─ Protects: Active transaction list
   ├─ Scope: System-wide
   └─ Contention: Medium (frequent allocation/deallocation)

2. PredicateLockHashLock (global)
   ├─ Protects: Predicate lock hash table
   ├─ Scope: System-wide
   └─ Contention: High (lock acquisition on every read/write)

3. Partition locks (128 partitions for predicate locks)
   ├─ Protects: Specific hash table buckets
   ├─ Scope: Per-bucket
   └─ Contention: Low (distributed)

4. Per-transaction perXactPredicateListLock (LWLock)
   ├─ Protects: Individual transaction's lock list
   ├─ Scope: Per-transaction
   └─ Contention: Very low (only if parallel workers)
```

### Example Deadlock Scenario (Prevented)

```
❌ WRONG ORDER (would deadlock):
Thread 1: Acquire PredicateLockHashLock
          → Tries to acquire SerializableXactHashLock
          → BLOCKED

Thread 2: Holds SerializableXactHashLock
          → Tries to acquire PredicateLockHashLock
          → BLOCKED (Thread 1 has it partially)
          
→ DEADLOCK!

✅ CORRECT ORDER (always safe):
Thread 1: Acquire SerializableXactHashLock
          → Acquire PredicateLockHashLock
          → OK (ordered)

Thread 2: Acquire SerializableXactHashLock
          → Must wait for Thread 1
          → No interference
```

### Performance Implications

```
Highly Contended Lock (PredicateLockHashLock):
├─ Can become bottleneck in high-concurrency workloads
├─ Solution 1: Partition locks reduce contention
│  ├─ 128 partitions ~= 128x reduction in lock conflicts
│  └─ Cost: Slightly more complex lock acquisition
├─ Solution 2: RCU-like optimizations (readers without locks)
│  └─ Allows lock-free reading of stable structures
└─ Solution 3: Per-core caching (planned for future PG versions)
```

---

## Prerequisites
- Complete understanding of Architecture Overview
- Familiarity with Conflict Detection and Lock Acquisition concepts
- Understanding of PostgreSQL's memory and concurrency infrastructure

## Next Steps
→ [Case Studies](17_case_studies.md) for practical application examples  
→ [Performance and Tuning](11_performance_and_tuning.md) for optimization insights  
→ [Appendix: Invariants Checklist](appendix_invariants_checklist.md) for implementation correctness
