# PostgreSQL SSI: Observability, Performance, Error Handling and Extensibility

## Observability and Debugging

### SQL-Visible Predicate Locks (pg_locks)

**Implementation**: `src/backend/catalog/system_views.sql`

```sql
SELECT * FROM pg_locks 
WHERE locktype = 'predicate'
LIMIT 10;
```

**Output columns** (for predicate locks):
- `pid`: Backend process ID
- `usesysid`: User OID
- `database`: Database OID
- `relation`: Relation OID (0 for relation-level)
- `page`: Page number (0 for page-level, InvalidBlockNumber for relation)
- `tuple`: Tuple offset (0 for tuple-level)
- `virtualxid`: Virtual transaction ID
- `transactionid`: Transaction XID
- `classid`, `objid`, `objsubid`: For extensible lock types
- `locktype`: Always 'predicate'
- `database`: Database containing lock
- `granted`: Always 't' (predicate locks never block)
- `fastpath`: Always 'f'

### Debug Support Functions

**GetPredicateLockStatusData()**:
```c
PredicateLockData *GetPredicateLockStatusData(void) {
    // Return snapshot of all predicate locks
    // Used by pg_locks view
    // Exported via SPI interface
}
```

**PageIsPredicateLocked()**:
```c
bool PageIsPredicateLocked(Relation relation, BlockNumber blkno) {
    // Check if page has predicate locks
    // Used by CLUSTER, VACUUM for optimization
}
```

### Logging Output

```c
// In predicate.c with PREDICATE_LOCK_DEBUG enabled:
ereport(DEBUG2,
    (errmsg("Predicate lock: %s %u/%u/%u/%u for %u",
            "acquired",
            tag.locktag_field1, tag.locktag_field2,
            tag.locktag_field3, tag.locktag_field4,
            sxact->topXid)));

// Dangerous structure detection:
ereport(DEBUG2,
    (errmsg("Dangerous structure: %u -> %u -> %u",
            tin_id, tpivot_id, tout_id)));

// Serialization failure:
ereport(ERROR,
    (errcode(ERRCODE_SERIALIZATION_FAILURE),
     errmsg("could not serialize access due to concurrent update")));
```

### trace_locks GUC (internal debugging)

```c
// Internal: controlled via #define, not user-facing
#ifdef TRACE_LOCKS
    trace_lock(op, relation, tid);
#endif
```

---

## Performance Analysis

### Time Complexity Summary

| Operation | Complexity | Lock Held |
|-----------|-----------|-----------|
| CheckForSerializableConflictOut | O(1) avg, O(k) worst | partition |
| CheckForSerializableConflictIn | O(m) | partition |
| OnConflict_CheckForSerializationFailure | O(d) | finished list |
| PredicateLockAcquire | O(1) | partition |
| ClearOldPredicateLocks | O(n*k) | finished list |
| GetSafeSnapshot | O(n) per loop | finished list |

Where:
- k = predicate locks on transaction
- m = locks on target being written
- n = finished transactions
- d = depth of conflict search

### Memory Characteristics

**Per-transaction overhead**:
- SERIALIZABLEXACT: 200 bytes
- Per predicate lock: 64 bytes
- Per conflict: 48 bytes
- Per xid mapping: 32 bytes

**Typical workload** (100 concurrent serializable transactions):
- Average 5 locks per transaction
- Average 2 conflicts per transaction

```
Total = 100 * (200 + 5*64 + 2*48 + 32)
      = 100 * (200 + 320 + 96 + 32)
      = 100 * 648
      = 64.8 KB
```

### Lock Contention Reduction

**Partition locking reduces contention by ~16x**:
- Without partitioning: All backends contend on single lock
- With 16 partitions: Average 6.25% of backends share lock
- Multiple backends can acquire different partitions simultaneously

**Example improvement**:
```
Single lock: 100 backends all waiting on 1 lock
Partitioned: 100 backends distributed across 16 locks
             → Only ~6 backends per lock on average
             → 16x throughput improvement (ideally)
```

### Query Performance Impact

**SSI adds minimal overhead for non-conflicting workloads**:
- Non-SERIALIZABLE isolation: 0% overhead
- SERIALIZABLE with no conflicts: ~2-5% overhead
- SERIALIZABLE with conflicts: Variable (depends on dangerous structures)
- Conflict detection: Mostly O(1) operations

---

## Error Modes and Retry Semantics

### Serialization Failure Error

```
SQLSTATE: 40001
Severity: ERROR
Message: "could not serialize access due to concurrent update"
```

**When thrown**:
1. At PreCommit_CheckForSerializationFailure() (commit time)
2. At MarkSxactDoomed() (when conflict becomes real)
3. When executor processes interrupt flag

**Application handling**:
```python
# Pseudocode - typical retry pattern
for attempt in range(MAX_RETRIES):
    try:
        with connection.transaction(
            isolation_level=SERIALIZABLE):
            # ... transaction logic ...
            connection.commit()
        break  # Success!
    except SerializationFailure:
        if attempt < MAX_RETRIES - 1:
            time.sleep(backoff_ms)
        else:
            raise
```

### Out of Memory Errors

```
ERRCODE_OUT_OF_MEMORY:
"not enough predicate lock memory"
OR
"not enough RWConflict pool entries"

Solution:
1. Reduce max_connections
2. Reduce concurrent serializable transactions
3. Reduce max_predicate_locks
4. Break work into smaller transactions
```

### Other Potential Errors

```c
// Partition lock deadlock (should never occur with strict ordering)
ERRCODE_LOCK_NOT_AVAILABLE

// Configuration error  
ERRCODE_INVALID_PARAMETER_VALUE
// if max_predicate_locks < max_connections

// Resource exhaustion
ERRCODE_OUT_OF_MEMORY
```

---

## Tuning for Performance

### GUC Parameter Guidelines

**For OLTP workloads** (many short transactions):
```ini
max_connections = 200
max_predicate_locks_per_transaction = 256  # More locks, fewer promotions
serializable_buffers = 128  # Larger SLRU
```

**For OLAP workloads** (few long transactions):
```ini
max_connections = 50
max_predicate_locks_per_transaction = 64   # Smaller, aggressive promotion
serializable_buffers = 32   # Smaller SLRU
```

**For mixed workloads**:
```ini
max_connections = 100
max_predicate_locks_per_transaction = 128
serializable_buffers = 64
```

### Identifying Performance Issues

```sql
-- Check for excessive lock promotion
SELECT count(*) as predicate_locks
FROM pg_locks WHERE locktype = 'predicate'
GROUP BY database;

-- If > max_predicate_locks/10, tune parameters

-- Monitor serialization failures
SELECT count(*) as failures
FROM pg_stat_statements
WHERE query LIKE '%SERIALIZATION_FAILURE%';

-- If > 1-2%, consider isolation level adjustment
```

---

## Error Modes and Retries

### False Positive Rate

**Definition**: Transaction rolled back even though no anomaly would have occurred.

**Expected rate**: ~1-5% of dangerous structures detected

**Why false positives exist**:
- SSI detects the pattern Tin → Tpivot → Tout
- But not all patterns embed in actual cycles
- Conservative approach: abort to guarantee correctness

**Mitigation**:
- Application layer retries
- Exponential backoff
- Consider lower isolation level for retryable operations

### Real Serialization Anomalies Prevented

**Example anomaly SSI prevents**:
```
Initial state: account A has balance 100, account B has balance 100

T1 (Serializable):
├─ Read A = 100
├─ Read B = 100
└─ Transfer 10 from A to B: SET A=90, SET B=110

T2 (Serializable):
├─ Read A = 90
├─ Read B = 100
└─ Transfer 10 from B to A: SET B=90, SET A=100

Without SSI:
├─ T1 completes: A=90, B=110
└─ T2 completes: A=100, B=90
Result: Total 190 (lost 10!)

With SSI:
├─ One transaction aborted with SERIALIZATION_FAILURE
└─ Retry succeeds: Total 100 (correct)
```

---

## Monitoring and Observability

### Key Metrics to Track

```c
// Per-transaction metrics
struct {
    long locks_acquired;
    long locks_promoted;
    long conflicts_detected;
    bool serialization_failure;
    long retry_count;
} TxnMetrics;

// System-level metrics  
struct {
    long total_serializable_txns;
    long total_conflicts;
    long total_dangerous_structures;
    long total_failures;
    long predicate_lock_peak;
} SystemMetrics;
```

### Monitoring Queries

```sql
-- Most locked relations
SELECT relation::regclass, count(*) as lock_count
FROM pg_locks WHERE locktype = 'predicate'
GROUP BY relation ORDER BY count DESC LIMIT 10;

-- Active serializable transactions
SELECT pid, usename, xact_start, state
FROM pg_stat_activity
WHERE iso_level = 'serializable';

-- Predicate lock distribution
SELECT locktype, count(*) as count
FROM pg_locks GROUP BY locktype;
```

---

## Hooks and Extensibility

### Module Hooks

PostgreSQL allows modules to extend SSI behavior:

```c
// Hook: called when predicate lock acquired
void (*PredicateLock_AddLockHook)(
    PREDICATELOCKTARGETTAG *tag,
    SERIALIZABLEXACT *sxact);

// Hook: called when conflict detected
void (*PredicateLock_ConflictHook)(
    SERIALIZABLEXACT *reader,
    SERIALIZABLEXACT *writer);

// Usage: custom conflict logging, alternative algorithms, etc.
```

### Extension Points

```c
// 1. Custom conflict detection
// Modules can register alternative dangerous structure detectors

// 2. Custom cleanup policies
// Alternative to lock promotion strategies

// 3. Custom storage backends
// For predicate lock targets (currently only in-memory)

// 4. Custom monitoring
// Export to external systems
```

### Example Extension Use Cases

```c
// 1. Conflict monitoring for data lineage
PredicateLock_ConflictHook() {
    // Log all conflicts to separate table
    // Analyze transaction dependencies
}

// 2. Alternative lock promotion
CheckAndPromotePredicateLockRequest_Hook() {
    // Use different promotion strategy
    // E.g., based on query patterns
}

// 3. Application-specific handling
GetSerializableTransactionSnapshot_Hook() {
    // Custom snapshot selection
    // E.g., based on transaction class
}
```

---

## Known Limitations

### Current Limitations

1. **Foreign Data Wrapper (FDW) Coverage**: Predicate locks only on local tables
2. **Unlogged Tables**: Not supported with SSI (would break recovery guarantees)
3. **Temporary Relations**: SSI support is per-backend
4. **Window Functions**: Lock all rows in frame (coarse-grained)
5. **Cursor Loops**: Multiple snapshots can reduce efficiency

### Workarounds

```sql
-- Instead of FDW reads in serializable:
-- Use explicit table copy with serializable guarantees

-- Instead of unlogged tables:
-- Use regular tables if SSI needed

-- For window functions:
-- Consider breaking into separate queries

-- For cursor loops:
-- Use explicit transaction boundaries
```

---

## Future Enhancements

### Potential Improvements

1. **Smarter Lock Promotion**: Machine learning-based decision
2. **FDW Integration**: Predicate locks for foreign tables
3. **Bloom Filter Optimization**: Reduce memory for large scans
4. **Parallel Conflict Detection**: Offload to background workers
5. **Hierarchical Lock Targets**: More granular than current 3 levels

### Research Directions

- False positive reduction algorithms
- Alternative dangerous structure patterns
- Adaptive transaction isolation levels
- Integration with distributed SSI protocols

