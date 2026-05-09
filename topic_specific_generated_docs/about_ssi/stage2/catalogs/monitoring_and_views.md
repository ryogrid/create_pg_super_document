# SSI Monitoring and Views Catalog

## SQL System Views

### pg_locks Integration

**View**: `pg_locks`  
**Schema**: `pg_catalog`

**Relevant Columns for SSI**:

```sql
SELECT 
    locktype,           -- 'predicate' for SSI locks
    database,           -- Database OID
    relation,           -- Relation OID
    page,               -- Page number (if page lock)
    tuple,              -- Tuple offset (if tuple lock)
    virtualxid,         -- Virtual transaction ID (locker)
    transactionid,      -- Transaction ID
    pid,                -- Process ID of locker
    mode,               -- 'Predicate' for all SSI locks
    granted             -- Always 't' (SSI locks never block)
FROM pg_locks
WHERE locktype = 'predicate';
```

**Granularity Encoding**:

| locktype | database | relation | page | tuple | Meaning |
|----------|----------|----------|------|-------|---------|
| predicate | db_oid | rel_oid | NULL | NULL | Relation-level lock |
| predicate | db_oid | rel_oid | blk# | NULL | Page-level lock |
| predicate | db_oid | rel_oid | blk# | offset# | Tuple-level lock |

**Example Queries**:

```sql
-- All predicate locks
SELECT * FROM pg_locks WHERE locktype = 'predicate';

-- Locks by current connection
SELECT * FROM pg_locks 
WHERE locktype = 'predicate' 
AND pid = pg_backend_pid();

-- Locks on specific table
SELECT * FROM pg_locks 
WHERE locktype = 'predicate' 
AND relation = 'my_table'::regclass;

-- Count by granularity
SELECT 
    CASE 
        WHEN page IS NULL THEN 'Relation'
        WHEN tuple IS NULL THEN 'Page'
        ELSE 'Tuple'
    END as granularity,
    COUNT(*) as count
FROM pg_locks 
WHERE locktype = 'predicate'
GROUP BY granularity;
```

---

## C Functions for Monitoring

### 1. GetPredicateLockStatusData()

**Source**: `./src/backend/storage/lmgr/predicate.c:3000`  
**Importance**: 0.88

**Signature**:
```c
PredicateLockData *GetPredicateLockStatusData(void)
```

**Purpose**: Export predicate locks for pg_locks view

**Returns**: PredicateLockData structure containing:
- All active predicate locks
- For each lock: target, xid, pid, mode
- Snapshot timestamp

**Called From**: lockfuncs.c `pg_lock_status()`

**Usage**:
```c
PredicateLockData *locks = GetPredicateLockStatusData();

for (int i = 0; i < locks->nelements; i++) {
    PredicateLockStatusElement *lock = locks->elements[i];
    // lock->database, lock->relation, lock->page, lock->tuple
    // lock->transactionid, lock->pid, lock->mode
}
```

---

### 2. PageIsPredicateLocked()

**Source**: `./src/backend/storage/lmgr/predicate.c:3050`  
**Importance**: 0.75

**Signature**:
```c
bool PageIsPredicateLocked(
    Relation relation,
    BlockNumber blkno)
```

**Purpose**: Check if page has any predicate locks

**Returns**: 
- TRUE: Page has locks
- FALSE: Page has no locks

**Use Cases**:
- Planning decisions
- Optimization checks
- Debug purposes

---

### 3. GetSafeSnapshot()

**Source**: `./src/backend/storage/lmgr/predicate.c:890`  
**Importance**: 0.90

**Signature**:
```c
Snapshot GetSafeSnapshot(void)
```

**Purpose**: Determine if read-only transaction can be served without locks

**Returns**:
- Valid snapshot if safe
- NULL if not safe (must acquire locks)

**Used By**: Read-only safe transaction optimization

---

## GUC Parameters

### 1. max_predicate_locks

**Type**: Integer  
**Unit**: Predicate locks  
**Default**: 64  
**Min**: 10  
**Max**: INT_MAX

**Purpose**: Maximum number of predicate locks allowed system-wide

**Behavior**:
- When exceeded: Promotes fine locks to coarser granularity
- Threshold triggers lock coalescing algorithm
- Setting too low: More promotion, less accurate conflict detection
- Setting too high: More memory usage, potentially better accuracy

**Recommendation**:
```ini
# OLTP: 64-256
# OLAP: 512-2048
# High-contention: 128-512
```

**Tuning**:
```sql
-- Check current usage
SHOW max_predicate_locks;

-- Check if hits limit (look for promotion messages)
SELECT * FROM pg_stat_statements 
WHERE query LIKE '%predicate%' AND mean_time > 10;
```

---

### 2. max_predicate_locks_per_transaction

**Type**: Integer  
**Unit**: Locks per transaction  
**Default**: 64  
**Min**: 10  
**Max**: INT_MAX

**Purpose**: Maximum predicate locks any single transaction can hold

**Behavior**:
- Per-transaction limit (stricter than global)
- Enforced at lock acquisition time
- Forces promotion when exceeded

**Recommendation**: Typically same as max_predicate_locks

---

### 3. max_predicate_locks_per_relation

**Type**: Integer  
**Unit**: Locks per relation  
**Default**: 64  
**Min**: 10  
**Max**: INT_MAX

**Purpose**: Maximum predicate locks on single relation

**Behavior**:
- When exceeded: Promote all to relation lock
- Very aggressive limit → fewer, coarser locks

**Recommendation**: 64 for most workloads

---

### 4. serializable_buffers

**Type**: Memory size  
**Unit**: Kilobytes  
**Default**: 16 (64KB)  
**Min**: 4 (16KB)  
**Max**: 1GB+

**Purpose**: Amount of shared memory for SSI structures

**Calculation**:
```
Total needed = 
  (max_predicate_locks * sizeof(PREDICATELOCK))
  + (max_connections * sizeof(SERIALIZABLEXACT))
  + (conflict pool)
  + (SLRU overhead)
```

**Recommendation**:
```ini
# Small system (1-10 connections)
serializable_buffers = 64

# Medium system (50-100 connections)
serializable_buffers = 256

# Large system (1000+ connections)
serializable_buffers = 1024
```

---

## Debug Functions

### 1. Log-Based Monitoring

**Parameter**: `log_lock_waits` (for regular locks, also useful context)

**Parameter**: `log_statement = 'all'` (log all statements)

**Parameters**: `log_min_duration_statement`

**Example**:
```ini
# postgresql.conf
log_statement = 'all'
log_min_duration_statement = 1000  # 1 second

# In logs, look for:
# ERROR: could not serialize access due to concurrent update
# NOTICE: (at statement X of transaction)
```

---

### 2. Error Message Logging

**Log Entry Format**:
```
ERROR: could not serialize access due to concurrent update
CONTEXT: while executing query on database "mydb"
```

**Parse Strategy**:
```bash
# Extract failed transaction patterns
grep "serialization_failure" postgresql.log | \
  awk -F'|' '{print $NF}' | \
  sort | uniq -c | sort -rn
```

---

## Diagnostic Queries

### Query 1: Current Lock Status

```sql
-- What locks are held right now?
SELECT 
    l.locktype,
    l.database,
    l.relation,
    l.page,
    l.tuple,
    l.transactionid,
    l.pid,
    p.usename,
    p.query_start,
    extract(epoch from (now() - p.query_start)) as duration_seconds
FROM pg_locks l
JOIN pg_stat_activity p ON l.pid = p.pid
WHERE l.locktype = 'predicate'
ORDER BY p.query_start DESC;
```

---

### Query 2: Lock Pressure

```sql
-- Are we hitting predicate lock limits?
SELECT 
    count(*) as total_locks,
    max_predicate_locks,
    round(100.0 * count(*) / 
          (SELECT setting::int FROM pg_settings 
           WHERE name = 'max_predicate_locks'), 2) as utilization_pct
FROM pg_locks
WHERE locktype = 'predicate'
GROUP BY max_predicate_locks;
```

---

### Query 3: Conflict Patterns

```sql
-- Which transactions have conflicts?
-- (Requires custom extension for visibility)

-- Monitor for serialization failures in log
SELECT 
    count(*) as failure_count,
    extract(hour from timestamp) as hour
FROM pg_log
WHERE message LIKE '%serialization_failure%'
GROUP BY extract(hour from timestamp)
ORDER BY hour DESC;
```

---

### Query 4: High-Conflict Transactions

```sql
-- Transactions doing reads then writes (high conflict risk)
SELECT 
    p.pid,
    p.usename,
    p.query_start,
    COUNT(CASE WHEN l.locktype = 'predicate' THEN 1 END) as pred_locks
FROM pg_stat_activity p
LEFT JOIN pg_locks l ON p.pid = l.pid
WHERE p.state = 'active'
GROUP BY p.pid, p.usename, p.query_start
HAVING COUNT(CASE WHEN l.locktype = 'predicate' THEN 1 END) > 0
ORDER BY pred_locks DESC;
```

---

## Performance Tuning

### Symptom 1: High Serialization Failure Rate

```sql
-- Check logs
SELECT count(*) as failures
FROM pg_log 
WHERE message LIKE '%serialization_failure%'
AND timestamp > now() - interval '1 hour';
```

**Possible causes**:
1. Too many concurrent reads → writes
2. Hot tables with many conflicts
3. Long-running read-only transactions
4. Contended data regions

**Fixes**:
- Increase isolation level to REPEATABLE READ for some queries
- Denormalize/partition to reduce conflicts
- Optimize query patterns to reduce overlap
- Use connection pooling with retry logic

---

### Symptom 2: High Lock Promotion

**Indicator**: Relation-level locks instead of tuple-level

```sql
-- Check lock granularity distribution
SELECT 
    CASE 
        WHEN page IS NULL AND tuple IS NULL THEN 'Relation'
        WHEN tuple IS NULL THEN 'Page'
        ELSE 'Tuple'
    END as granularity,
    count(*) as count
FROM pg_locks
WHERE locktype = 'predicate'
GROUP BY granularity;
```

**If mostly Relation-level**:
- Increase `max_predicate_locks`
- Increase `max_predicate_locks_per_transaction`
- Increase `max_predicate_locks_per_relation`

**Trade-off**: Memory vs. conflict detection accuracy

---

### Symptom 3: Memory Pressure

**Indicator**: Frequent promotion, forced lock coalescing

**Check**:
```sql
SELECT setting FROM pg_settings 
WHERE name = 'serializable_buffers';
```

**Fix**: Increase `serializable_buffers` in postgresql.conf

**Impact**: Server restart required

---

## Extension: Custom Monitoring Extension

**Example Extension Structure**:

```c
// ssi_monitor.c - Track conflict patterns
#include "postgres.h"
#include "storage/predicate_internals.h"

static RWConflictDetectedHook_type prev_hook = NULL;

static void
my_conflict_logger(const SERIALIZABLEXACT *reader,
                  const SERIALIZABLEXACT *writer,
                  bool write_after_read) {
    // Log conflict to table
    // Categorize by tables involved
    // Track conflict rates
    
    if (prev_hook)
        prev_hook(reader, writer, write_after_read);
}

void _PG_init(void) {
    prev_hook = RWConflictDetectedHook;
    RWConflictDetectedHook = my_conflict_logger;
}
```

---

## Best Practices for Monitoring

1. **Log Serialization Failures**
   - Enable error logging
   - Parse to identify patterns
   - Set up alerts on threshold

2. **Track Lock Utilization**
   - Query pg_locks periodically
   - Track promotion trends
   - Monitor peak lock counts

3. **Performance Correlation**
   - Measure SSI overhead (2-5% typical)
   - Alert if overhead exceeds threshold
   - Correlate with query patterns

4. **Capacity Planning**
   - Track max lock count reached
   - Reserve headroom (80% max)
   - Plan for growth

