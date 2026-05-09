# SSI Quick Reference Card

**Two-page summary of Serializable Snapshot Isolation concepts, patterns, and tuning.**

---

## Page 1: Concepts & Key Terms

### What is SSI?
PostgreSQL's **Serializable Snapshot Isolation** provides ACID serializability without two-phase locking.
- Uses **snapshots** for readers (no blocking)
- Uses **predicate locks** to detect conflicts
- Uses **dangerous structure detection** to prevent anomalies
- Introduced in PostgreSQL 9.1

### SSI vs. Snapshot Isolation
**Snapshot Isolation** (SI): Readers see consistent snapshot, but allows serialization anomalies.
**SSI**: SI + dangerous structure detection = true serializability.

| Aspect | SI | SSI |
|--------|----|----|
| Reader blocking | No | No |
| Serialization violations | Yes | No |
| Complexity | Low | High |
| Overhead | None | ~1-5% typical |

### The Dangerous Structure Pattern
```
Tin (writes) → Tpivot (reads, writes) → Tout (reads)
                                           ↓
                                    Tin (reads)
                                    
Creates cycle: Tin-Tpivot-Tout-Tin
SSI detects & aborts one transaction before commitment.
```

### Lock Granularities
```
Fine-grained (specific rows):     TUPLE locks
Medium (page of rows):             PAGE locks  
Coarse (entire table):             RELATION locks

Memory pressure: Auto-promote
TUPLE (256) → PAGE → RELATION lock
```

### Read-Only Optimization
- **Safe snapshot**: RO txn that never conflicts
- **Deferrable**: `SERIALIZABLE DEFERRABLE` waits for safety
- **Benefit**: Zero overhead for RO transactions
- **Cost**: Initial latency, guaranteed success

---

## Page 2: Common Patterns & Tuning

### Retry Pattern (MUST-KNOW)
```python
MAX_RETRIES = 5
retry_count = 0

while retry_count < MAX_RETRIES:
    try:
        conn.begin()  # or:
        conn.set_isolation_level(SERIALIZABLE)
        
        # Your transaction code here:
        # SELECT, INSERT, UPDATE, DELETE
        
        conn.commit()
        break
    except psycopg2.OperationalError as e:
        if "40001" in str(e):  # SERIALIZATION_FAILURE
            conn.rollback()
            retry_count += 1
            if retry_count < MAX_RETRIES:
                time.sleep(0.01 * (2 ** retry_count))  # Exponential backoff
            else:
                raise SerializationFailureError("Max retries exceeded")
        else:
            raise
```

### Deferrable Read-Only (For Analytics)
```sql
-- Long-running analytical query guaranteed zero failures
BEGIN TRANSACTION 
    ISOLATION LEVEL SERIALIZABLE 
    DEFERRABLE;

SELECT COUNT(*), AVG(amount), MAX(amount)
FROM transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days';

COMMIT;
```

### Application-Level Ordering (Reduce Conflicts)
```sql
-- Always update in consistent order (lowest ID first)
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
-- vs. (non-deterministic order leads to more conflicts)
UPDATE accounts SET balance = balance - 100 WHERE id = ?;
UPDATE accounts SET balance = balance + 100 WHERE id = ?;
```

### GUC Parameters (Tuning)
```sql
-- High concurrency workloads
ALTER SYSTEM SET max_predicate_locks = 1000000;

-- Large scanning workloads
ALTER SYSTEM SET max_predicate_locks_per_transaction = 256;

-- Per-relation coalescing threshold
ALTER SYSTEM SET max_predicate_locks_per_relation = -1;  -- Unlimited

-- Apply changes
SELECT pg_ctl_reload_conf();
```

### Monitoring Serialization Failures
```sql
-- Enable logging
ALTER SYSTEM SET log_min_error_statement = 'NOTICE';
ALTER SYSTEM SET log_statement = 'mod';

-- Check log
tail -f /var/log/postgresql/postgresql.log | grep "40001"

-- Application-level: count retries per query
SELECT query, count(*) as retry_count 
FROM application_retry_log
GROUP BY query
ORDER BY retry_count DESC;
```

---

## Command Reference

### Transaction Control
```sql
BEGIN;
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE DEFERRABLE;
COMMIT;
ROLLBACK;
SAVEPOINT sp1;
RELEASE SAVEPOINT sp1;
```

### Information Queries
```sql
-- Show isolation level
SHOW transaction_isolation;

-- Show SSI parameters
SHOW max_predicate_locks;
SHOW max_predicate_locks_per_transaction;
SHOW max_predicate_locks_per_relation;

-- View active transactions (simple)
SELECT pid, usename, query, state FROM pg_stat_activity;

-- View active transactions with timing
SELECT 
    pid, usename, state,
    xact_start, query_start,
    EXTRACT(EPOCH FROM (NOW() - xact_start)) as txn_seconds
FROM pg_stat_activity
WHERE state != 'idle';
```

### Error Handling
```sql
-- SQLSTATE 40001 = serialization_failure
-- Must catch at application level and retry

-- Example in PL/pgSQL
BEGIN
    INSERT INTO table VALUES (...);
    COMMIT;
EXCEPTION WHEN serialization_failure THEN
    RAISE NOTICE 'Serialization failure, please retry';
END;
```

---

## Performance Tips

### ✓ Do This
- Use SERIALIZABLE for isolation requirements
- Implement retry logic with exponential backoff
- Use DEFERRABLE for long RO queries
- Order operations consistently
- Keep transactions short
- Batch small operations
- Use REPEATABLE READ for RO-safe queries

### ✗ Don't Do This
- Catch SQLSTATE 40001 without retry
- Use long-running transactions under high load
- Acquire locks outside of transactions
- Perform I/O inside transactions
- Ignore serialization failures
- Use random backoff times

---

## Quick Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|----------|
| Frequent SQLSTATE 40001 | High contention | Increase `max_predicate_locks`, reduce transaction duration |
| Memory usage growing | Too many locks | Decrease `max_predicate_locks`, enable coalescing |
| SERIALIZABLE transactions slow | Lock overhead | Use `SERIALIZABLE DEFERRABLE` for RO, or `REPEATABLE READ` |
| Predictable patterns of failures | Dangerous structures | Review and order transaction access patterns |
| One transaction always aborted | Abort victim chosen | Implement proper retry with exponential backoff |

---

## References

- **Isolation Levels**: `SET TRANSACTION ISOLATION LEVEL`
- **SSI Tuning**: Chapter 11 (Performance and Tuning)
- **Error Handling**: Chapter 12 (Error Modes and Retries)
- **Deep Algorithm**: Chapter 18 (Deep Dives)
- **Case Studies**: Chapter 17 (Case Studies)

---

**Printed from**: PostgreSQL SSI Technical Manual  
**Version**: 9.1+  
**Last Updated**: May 2026
