# Executive Summary: PostgreSQL Serializable Snapshot Isolation

## What is SSI?

PostgreSQL's **Serializable Snapshot Isolation (SSI)** is a transaction isolation algorithm that provides true ACID serializability without the performance overhead of traditional two-phase locking. SSI was introduced in PostgreSQL 9.1 and implements the algorithm described in "Serializable Isolation for Snapshot Databases" (Cahill et al., 2008).

## The Problem: Snapshot Isolation Anomalies

Standard **Snapshot Isolation (SI)** provides each transaction with a consistent view of the database at a specific point in time. While this prevents dirty reads and phantom reads, it allows **serialization anomalies** — execution patterns that violate serializability:

```
T1: SELECT COUNT(*) FROM accounts WHERE balance > 10000
    -- Result: 10 accounts

T2: UPDATE accounts SET balance = balance - 15000
    WHERE balance > 10000  -- Affects 5 accounts
    -- T2 commits

T1: SELECT COUNT(*) FROM accounts WHERE balance > 10000
    -- Result: 5 accounts (anomaly!)
```

In this example, T1's two queries conflict even though they never explicitly lock anything. The second query sees changes that affect data T1 already examined.

## The Solution: Dangerous Structure Detection

SSI prevents these anomalies using **dangerous structure detection**: an algorithm that identifies when three transactions (Tin, Tpivot, Tout) create a pattern that could violate serializability:

- **Tin** → **Tpivot**: Tin's write conflicts with Tpivot's read
- **Tpivot** → **Tout**: Tpivot's write conflicts with Tout's read

This pattern creates a cycle when combined with Tout → Tin, violating serializability. SSI detects this pattern **before commit** and aborts one transaction.

### Example
```
T1 (Tin):   UPDATE table1 SET x = x+1    -- writes
T2 (Tpivot): SELECT * FROM table1        -- reads T1's write
T3 (Tout):  UPDATE table1 SET x = x-1    -- writes
            SELECT * FROM table2          -- reads from original snapshot
T2: UPDATE table2 SET y = (SELECT x FROM table1)  -- reads T3's write
```

When T2 commits, SSI detects the dangerous structure and aborts either T2 or T3 to prevent the anomaly.

## Path vs Plan Duality

SSI's predicate locking mirrors the **query planner's path/plan concept**:

| Planner | SSI |
|---------|-----|
| **Path**: Access method option (SeqScan, IndexScan) | **Predicate**: Data affected by transaction (relation, page, tuple) |
| **Plan**: Selected, optimized execution path | **Lock**: Selected lock granularity (relation, page, tuple) |
| Optimizer searches path space to find best plan | Lock subsystem promotes locks when coalescing is beneficial |

Just as the planner balances cost vs flexibility in path selection, SSI balances lock granularity vs memory usage.

## Key Trade-offs

SSI makes strategic trade-offs:

| Aspect | Trade-off |
|--------|-----------|
| **False Positives** | SSI aborts some transactions that *could* have been serializable. Cost: unnecessary retries. Benefit: avoid complex analysis at commit time |
| **Read-Only Optimization** | Detects safe snapshots where RO transactions can never conflict. Cost: complex state tracking. Benefit: zero overhead for RO txns |
| **Lock Granularity** | Starts fine-grained (tuples), coalesces to coarse (relations) when memory pressure high. Cost: potential false conflicts. Benefit: bounded memory |
| **Snapshot Consistency** | Uses MVCC snapshot, not locking. Cost: predicate locks track *potential* conflicts. Benefit: readers never block |

## Why SSI Instead of 2PL?

| Feature | 2PL | SSI |
|---------|-----|-----|
| **Reader Blocking** | Yes (readers hold locks) | No (MVCC snapshots) |
| **Deadlocks** | Frequent | Rare (conflicts, not locks) |
| **Performance** | Lock contention limits throughput | High concurrency via snapshots |
| **Implementation** | Simpler | More complex (dangerous structure detection) |
| **Workload Fit** | OLTP with frequent writes | OLTP + OLAP, mixed read/write |

## Critical Concepts

### Dangerous Structure Pattern
Three-transaction pattern (Tin-Tpivot-Tout) that violates serializability. SSI detection prevents this.

### SIREAD Lock
Predicate locks used by SSI (stands for "Serializable Isolation Read"). Tracks predicates (ranges) rather than physical rows.

### Safe Snapshot
A snapshot where a read-only transaction can never conflict with any other transaction. Enables zero-overhead RO txns.

### Lock Promotion
Coalescing fine-grained locks (tuples) into coarser locks (pages, relations) when memory is constrained.

### Predicate Lock Target
The data item being locked: can be a relation, page, or tuple. Stored in shared memory hash tables.

## Impact on Application Code

For applications:

1. **Retry Pattern**: Applications must retry on `SQLSTATE 40001` (serialization failure)
   ```python
   while True:
       try:
           cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
           cursor.execute(... your transaction ...)
           conn.commit()
           break
       except serialization_error:
           conn.rollback()
           continue
   ```

2. **Idempotency**: Retried transactions must be idempotent (produce same result if executed multiple times)

3. **Deferrable Transactions**: Use `BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE DEFERRABLE` for long-running read-only operations to use safe snapshots

## Performance Characteristics

- **Read-Only Txns**: ~0% overhead (fast-path via safe snapshot detection)
- **Write Txns, High Contention**: 10-30% overhead (predicate lock tracking)
- **Write Txns, Low Contention**: 1-5% overhead
- **Memory**: Bounded by `max_predicate_locks` parameter (predictable)
- **Serialization Failures**: Rate depends on workload conflict patterns

## Documentation Roadmap

| When | Read This |
|------|-----------|
| **Learning SSI** | Executive Summary → Architecture Overview → Lifecycle and Entry Points |
| **Debugging** | Observability and Debugging → Error Modes and Retries → Deep Dives |
| **Implementing** | Architecture Overview → Deep Dives → Data Structures Catalog |
| **Tuning** | Performance and Tuning → Configuration Notes |
| **API Reference** | Predicate Lock APIs → Conflict APIs → Source Map |

---

## Next Steps

→ Read [Architecture Overview](02_architecture_overview.md) for system-wide perspective  
→ See [Quick Reference Card](ssi_quick_reference.md) for cheat sheet
