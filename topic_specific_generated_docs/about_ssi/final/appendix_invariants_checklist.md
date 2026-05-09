# Appendix E: Invariants Checklist

**Correctness properties and implementation guidelines for SSI implementers.**

---

## Core Correctness Invariants

### Invariant 1: Snapshot Consistency
**Statement**: Every SERIALIZABLE transaction operates from a consistent snapshot of the database.

**Verification**:
- [ ] Snapshot acquired once per transaction (before first operation)
- [ ] xmin, xmax, xip[] arrays immutable for transaction lifetime
- [ ] Snapshot's xmin ≤ any txn's xid that started before this one
- [ ] All tuples visible per snapshot remain consistent

**Implementation Check**:
```c
// In GetSerializableTransactionSnapshot()
Assert(snapshot.xmin != InvalidTransactionId);
Assert(snapshot.xmax >= snapshot.xmin);
Assert(snapshot.xcnt >= 0);
// Snapshot doesn't change after this point
```

### Invariant 2: Conflict Graph Acyclicity (Pre-Commit)
**Statement**: Before any transaction commits, there is no cycle in the conflict graph.

**Verification**:
- [ ] OnConflict_CheckForSerializationFailure() called on every potential cycle
- [ ] Cycle detection prevents Tin-Tpivot-Tout pattern
- [ ] No transaction commits if cycle would result
- [ ] At least one transaction is aborted per potential cycle

**Implementation Check**:
```c
// In OnConflict_CheckForSerializationFailure()
Assert(HasNoIncomingFromCommitted(reader) || 
       HasNoCycleToWriter(writer));
// If both false → ABORT one transaction
```

### Invariant 3: Predicate Lock Completeness
**Statement**: Every read or write creates predicate locks capturing the accessed data range.

**Verification**:
- [ ] Every heap scan acquires PredicateLockRelation() or PredicateLockPage()
- [ ] Every index scan acquires appropriate lock (via index handler)
- [ ] Every tuple access (read or write) captured by lock
- [ ] Coalescing: if tuple locks promoted, relation lock created

**Implementation Check**:
```c
// In CheckForSerializableConflictOut()
Assert(TransactionHasPredicateLockOn(relation, page, tuple) ||
       TransactionHasPredicateLockOn(relation, page, InvalidOffsetNumber) ||
       TransactionHasPredicateLockOn(relation, InvalidBlockNumber, InvalidOffsetNumber));
```

### Invariant 4: Dangerous Structure Detection (Complete)
**Statement**: SSI detects ALL dangerous structures before commit.

**Verification**:
- [ ] OnConflict_CheckForSerializationFailure() scans all possible Tin (incoming conflicts from committed txns)
- [ ] For each Tin, scans all outgoing from writer (Tout)
- [ ] Checks for path from Tout back to Tin via target conflicts
- [ ] False negatives: ZERO (all structures detected)
- [ ] False positives: Allowed (aborts some safe transactions)

**Implementation Check**:
```c
// Full dangerous structure check
for_each(inConflict C from committed txn T in reader.inConflicts) {
    for_each(outConflict W from writer) {
        if (T_can_reach_W.target_via_conflicts()) {
            // Dangerous structure found!
            AbortOne(reader, writer, T, W.target);
        }
    }
}
// Must check ALL combinations
```

### Invariant 5: Memory Boundedness
**Statement**: SSI memory usage is bounded by max_predicate_locks, regardless of workload.

**Verification**:
- [ ] num_predicate_locks ≤ max_predicate_locks (always)
- [ ] When limit approached, coalesce locks proactively
- [ ] Coalescing reduces lock count (tuple → page → relation)
- [ ] Summarization reduces in-memory transaction count

**Implementation Check**:
```c
// Before allocating new lock
if (PredicateLockTableTotalEntries() >= max_predicate_locks * 0.9) {
    PromotePredicateLocks();  // Coalesce, free memory
}
Assert(PredicateLockTableTotalEntries() < max_predicate_locks);
```

### Invariant 6: No Reader Blocking
**Statement**: SERIALIZABLE transactions never block on predicate locks (unlike 2PL).

**Verification**:
- [ ] PredicateLockRelation/Page/Tuple() never sleeps or waits
- [ ] Conflict detection is post-hoc, not preventive
- [ ] Writers don't block on reader predicate locks
- [ ] Only mechanisms: MVCC for snapshot visibility, serialization failure at commit

**Implementation Check**:
```c
// Lock acquisition must not block
void PredicateLockAcquire(...) {
    // LWLock acquisition (brief):
    Lock(PredicateLockHashLock);  // Non-blocking in single-server case
    
    // Hash lookup, allocation, insertion:
    existing = HashSearchOrInsert(...);  // O(1) operations
    
    // Return immediately:
    Unlock(PredicateLockHashLock);
    // No sleeps, no condition variables, no blocking
}
```

### Invariant 7: Idempotent Retries
**Statement**: When a transaction is retried after SERIALIZATION_FAILURE, it can be retried safely.

**Verification**:
- [ ] Application code must be idempotent (same operation, same result if retried)
- [ ] No side effects that can't be undone (e.g., sequence.nextval inside txn)
- [ ] Warnings in documentation clear about this requirement

**Implementation Note** (not code, application responsibility):
```python
# ✓ CORRECT: Idempotent operation
retry_count = 0
while retry_count < MAX_RETRIES:
    try:
        conn.begin()
        result = cursor.execute("SELECT balance FROM accounts WHERE id = 1")
        new_balance = result[0] - 100
        cursor.execute("UPDATE accounts SET balance = %s WHERE id = 1", new_balance)
        conn.commit()
        break
    except psycopg2.OperationalError as e:
        if "40001" in str(e):
            conn.rollback()
            retry_count += 1
        else:
            raise

# ✗ INCORRECT: Has side effects
retry_count = 0
while retry_count < MAX_RETRIES:
    try:
        conn.begin()
        seq_val = cursor.execute("SELECT NEXTVAL('myseq')")[0]  # Side effect!
        cursor.execute("INSERT INTO log (seq_val) VALUES (%s)", seq_val)
        conn.commit()
        break
    except:
        if "40001" in str(e):
            # Retrying will try to use different NEXTVAL!
            # Side effect not undone
```

---

## Transaction State Machine Invariants

### Invariant T1: SERIALIZABLEXACT State Transitions
**Valid transitions**:
```
CREATED → ACTIVE → COMMITTED ✓
CREATED → ACTIVE → DOOMED → (abort) ✓
(any state) → ROLLED_BACK (on explicit abort) ✓
COMMITTED → FINISHED → SUMMARIZED (cleanup) ✓
```

**Invalid transitions**:
```
COMMITTED → ACTIVE ✗  (transaction can't restart)
FINISHED → COMMITTED ✗ (can't commit finished txn)
DOOMED → COMMITTED ✗ (doomed txn must abort)
```

**Verification**:
- [ ] State transitions checked before mutations
- [ ] Invalid transitions cause Assert/ERROR
- [ ] Flag combinations maintained (e.g., COMMITTED and ROLLED_BACK mutually exclusive)

### Invariant T2: Lock Acquisition Ordering
**Valid lock acquisition ordering**:
```
SerializableXactHashLock → PredicateLockHashLock → PartitionLock
```

**Invariant**: If lock A is held, only locks ≥ A in order can be acquired.

**Verification**:
- [ ] Lock acquisition follows ordering (no reverse acquisition)
- [ ] No nested lock acquisition outside lock order
- [ ] Deadlock prevention maintained

**Implementation Check**:
```c
// If we're acquiring PredicateLockHashLock, we must have released
// any higher-order locks
Assert(!LockIsHeld(SerializableXactHashLock));

// Or we already hold SerializableXactHashLock
if (LockIsHeld(SerializableXactHashLock)) {
    Acquire(PredicateLockHashLock);  // OK
}
```

### Invariant T3: SERIALIZABLEXACT Lifetime
**Statement**: SERIALIZABLEXACT exists for duration of transaction + duration of potential future conflicts.

**Phases**:
```
1. ACTIVE: From snapshot acquisition until commit/abort
2. FINISHED: From commit until safe to summarize (~1 second)
3. SUMMARIZED: Compressed to SLRU for crash recovery
```

**Verification**:
- [ ] Active: while transaction running
- [ ] Finished: added to FinishedSerializableTransactions list
- [ ] Summarized: written to SLRU, removed from in-memory list
- [ ] Can re-check conflicts from SLRU summary even after SUMMARIZED

---

## Conflict Graph Invariants

### Invariant C1: Bidirectional Conflict Edges
**Statement**: If there's a conflict A→B (A writes, B reads), then B.inConflicts includes A, and A.outConflicts includes B.

**Verification**:
```c
// When creating conflict A→B:
AddToOutConflicts(A, B);      // A.outConflicts += B
AddToInConflicts(B, A);        // B.inConflicts += A

// Verify consistency:
Assert(B.has_in_conflict_from(A) == A.has_out_conflict_to(B));
```

### Invariant C2: No Self-Loops
**Statement**: A transaction cannot have a conflict with itself (A→A not possible).

**Verification**:
- [ ] reader != writer in CheckForSerializableConflictOut()
- [ ] reader != writer in CheckForSerializableConflictIn()
- [ ] source != target in all conflict checks

### Invariant C3: Committed Transaction Immutability
**Statement**: A committed transaction's conflict edges never change.

**Verification**:
- [ ] Once COMMITTED flag set, no new conflicts added
- [ ] Active transactions can add conflicts to committed txns (one-way)
- [ ] Committed transactions don't add conflicts (one-way edges only from active txns)

---

## Read-Only Optimization Invariants

### Invariant RO1: Safe Snapshot Correctness
**Statement**: If RO_SAFE flag is set, transaction will never see a serialization failure.

**Verification**:
- [ ] RO_SAFE only set if no active SERIALIZABLE txns started before this one
- [ ] All older txns either finished or will finish without conflicts
- [ ] Transaction is truly read-only (no writes)
- [ ] PreCommit_CheckForSerializationFailure() returns OK for RO_SAFE

**Implementation Check**:
```c
// Setting RO_SAFE must check all conditions:
if (IsReadOnlyTransaction() &&
    NoActiveTxnsStartedBefore(current_txn) &&
    NoRecentlyCommittedConflictingTxns(current_txn)) {
    SetFlag(current_txn, RO_SAFE);
    Assert(current_txn.will_always_commit());
}
```

### Invariant RO2: Read-Only Lock Optimization
**Statement**: If RO_SAFE, no predicate locks are acquired.

**Verification**:
- [ ] PredicateLockRelation() checks RO_SAFE, returns early if true
- [ ] PredicateLockPage() checks RO_SAFE, returns early if true
- [ ] PredicateLockTuple() checks RO_SAFE, returns early if true
- [ ] Reduced overhead: zero lock acquisition cost

---

## Serialization Failure Invariants

### Invariant S1: Deterministic Abort Decisions
**Statement**: Dangerous structure detection makes deterministic decisions about which transaction to abort.

**Verification**:
- [ ] Decision algorithm is deterministic (same inputs → same decision)
- [ ] Choice between txns: predictable (e.g., always abort younger)
- [ ] No randomness in abort decision
- [ ] Application can implement exponential backoff correctly

---

## Verification Checklist for Implementers

When implementing SSI for a new system, verify:

### Snapshot System
- [ ] Snapshot acquisition: Immutable for transaction lifetime
- [ ] Visibility checks: Based on snapshot xmin/xmax/xip[]
- [ ] Integration: MVCC uses same snapshot

### Predicate Locking
- [ ] Lock acquisition: Non-blocking, O(1)
- [ ] Lock completeness: All reads/writes captured
- [ ] Coalescing: Bounded memory via promotion
- [ ] Per-transaction limit: Enforced with promotion

### Conflict Detection
- [ ] Outgoing conflicts: On writes, check for readers
- [ ] Incoming conflicts: On reads, check for writers
- [ ] Bidirectional maintenance: A→B maintained correctly
- [ ] No self-loops: Never A→A

### Dangerous Structure Detection
- [ ] Complete scan: All possible Tin checked
- [ ] Cycle detection: Tin-Tpivot-Tout pattern found
- [ ] Deterministic abort: Reproducible decision
- [ ] No false negatives: All structures found

### Commit Validation
- [ ] Safe snapshots: RO_SAFE txns skip validation
- [ ] Dangerous structure check: Full scan performed
- [ ] Abort decision: Determined correctly
- [ ] Serialization failure exception: Raised properly

### Memory Management
- [ ] Bounded allocation: Never exceed max_predicate_locks
- [ ] Coalescing: Triggered at threshold
- [ ] Summarization: Old transactions compressed
- [ ] Crash recovery: SLRU properly restored

### Synchronization
- [ ] Lock ordering: Never violated
- [ ] No deadlocks: Consistent ordering maintained
- [ ] Correctness: No races on shared state
- [ ] Performance: Minimal contention on hot locks

---

## Testing Checklist

Before deploying SSI implementation:

### Unit Tests
- [ ] Snapshot immutability tests
- [ ] Predicate lock acquisition tests
- [ ] Conflict graph construction tests
- [ ] Dangerous structure detection tests
- [ ] Coalescing mechanism tests
- [ ] Memory bounding tests

### Integration Tests
- [ ] Multi-transaction conflict tests
- [ ] Serialization failure scenarios
- [ ] Read-only optimization tests
- [ ] 2PC integration tests
- [ ] Crash recovery tests
- [ ] Parallel query tests

### Performance Tests
- [ ] Lock acquisition overhead
- [ ] Commit validation latency
- [ ] Memory usage under load
- [ ] Scalability with concurrency
- [ ] False positive rates
- [ ] Serialization failure rates

---

## Related Resources

- [Deep Dives](18_deep_dives.md) - Algorithm details
- [Architecture Overview](02_architecture_overview.md) - System design
- [Source Map](appendix_source_map.md) - Implementation locations
