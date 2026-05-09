# PostgreSQL SSI: Performance Tuning and Optimization

## Performance Characteristics

### Algorithmic Complexity

**Dangerous structure detection**: O(d) where d = search depth
- Average case: O(1) - most transactions have 0-2 conflicts
- Worst case: O(n²) - densely connected conflict graph
- Practical: Most systems see O(1) behavior

**Predicate lock acquisition**: O(1) amortized
- Hash table lookup: O(1)
- Lock promotion: O(m) where m = locks on target
- Practical amortization: O(1) per lock

**Memory reclamation**: O(n·k) where n = finished transactions, k = locks/txn
- Linear scan of finished list
- Cleanup triggered periodically (not per-transaction)
- Incremental: can spread across multiple cleanup cycles

### Wall-Clock Performance

**SSI overhead** (vs. READ COMMITTED):
- No conflicts: 2-5% overhead
- Mild conflicts: 5-15% overhead
- Heavy conflicts: 20-50% overhead + retries
- Read-only safe: 1-2% overhead (highly optimized)

**Breakdown**:
```
Per-statement overhead:
├─ Predicate lock acquisition: ~1-2µs per lock
├─ Conflict check: ~0.5µs per conflict
├─ Dangerous structure scan: ~1-10µs (if conflicts exist)
└─ Memory promotion: ~5-50µs (if memory pressure)

Per-transaction overhead:
├─ Commit-time validation: ~10-100µs
├─ Lock cleanup: ~20-200µs
└─ SLRU recording: ~5-50µs
```

---

## GUC Parameter Tuning

### max_predicate_locks

**Description**: Maximum predicate locks in system
**Default**: (max_connections + max_prepared_xacts) * 64
**Range**: 10 - 2147483647

**Guidance**:
```
# Light OLTP (few serializable txns)
max_predicate_locks = 64000  # (100 connections * 64)

# Heavy OLTP (many serializable txns)
max_predicate_locks = 256000  # (400 connections * 64)

# OLAP (long transactions, many locks)
max_predicate_locks = 512000  # (1000 connections * 64)

# Monitoring: Check current usage
SELECT count(*) FROM pg_locks WHERE locktype = 'predicate';
# If approaching limit, transaction will be forced to abort
```

**Impact**:
- Larger value: More locks allowed, less promotion needed
- Smaller value: More aggressive lock promotion, less memory
- Too small: Frequent promotion reduces effectiveness

### max_predicate_locks_per_transaction

**Description**: Maximum locks per transaction
**Default**: 64
**Range**: 10 - 2147483647

**Guidance**:
```
# Tables with <100 rows scanned per transaction
max_predicate_locks_per_transaction = 64  (default)

# Tables with 100-1000 rows scanned
max_predicate_locks_per_transaction = 256

# Large table scans
max_predicate_locks_per_transaction = 512

# In-memory analytics
max_predicate_locks_per_transaction = 2048
```

**Derivation**:
```
Required estimate:
    = MAX(rows_scanned) * (bytes_per_tuple / page_size)
    
For 1000-row table with 100-byte rows:
    = 1000 * (100 / 8192)
    = 12 page-level locks
    
Add 50% safety factor:
    = 18 locks needed (use 64 default)
```

### max_predicate_locks_per_relation

**Description**: Maximum locks per relation
**Default**: max_predicate_locks_per_transaction / 10
**Range**: 1 - 2147483647

**Guidance**:
```
# Defaults to 1/10 of per-transaction limit

# If relation is frequently scanned in parts:
max_predicate_locks_per_relation = 100

# If relation often scanned entirely:
max_predicate_locks_per_relation = 10

# If small relations used as dimension tables:
max_predicate_locks_per_relation = 5
```

**Decision logic**:
```
if (locks_on_relation > max_predicate_locks_per_relation):
    # Promote all page locks → relation lock
    # Release fine-grained locks
    # Create single relation-level lock
```

### serializable_buffers

**Description**: SLRU buffer pool size for commit history
**Default**: 64 pages
**Range**: 1 - (shared_buffers / 1024)

**Guidance**:
```
# Small databases
serializable_buffers = 32

# Medium databases with moderate serializable activity
serializable_buffers = 64  (default)

# Large databases with heavy serializable workload
serializable_buffers = 256

# Very high concurrency
serializable_buffers = 512 or higher
```

**Calculation**:
```
# SLRU page = 8192 bytes
Memory per 64 buffers = 64 * 8192 = 512 KB

# Double buffers = roughly double memory
# Each buffer: ~8-10 KB depending on content
```

---

## Query Plan Optimization

### Lock Granularity Impact

**Sequential scan**:
```
CREATE TABLE accounts (id INT, balance INT);

-- Full table scan acquires RELATION lock
SELECT * FROM accounts;
-- Result: 1 lock (relation-level)
-- Cost: Low memory, fast conflict check

-- vs. index access
SELECT * FROM accounts WHERE id = 1;
-- Result: 1 lock (tuple-level)
-- Cost: Higher memory, slower conflict check
```

**Index scan**:
```
-- B-tree index scan acquires locks along path
SELECT * FROM accounts WHERE id BETWEEN 100 AND 200;
-- Result: 0-200 locks (tuple-level)
-- Cost: Highest memory, slowest if many tuples

-- If too many locks
-- Automatically promoted to page locks
-- Then potentially to relation locks
```

### Planner Hints

```sql
-- Force seq scan for relation locks
/*+ SeqScan(accounts) */
SELECT * FROM accounts WHERE balance > 100;

-- Force index scan for finer-grained locks
/*+ IndexScan(accounts idx_balance) */
SELECT * FROM accounts WHERE balance > 100;

-- Note: Query planner doesn't currently optimize for SSI
-- But can influence via statistics adjustment
```

### Statistics Impact

```sql
-- More accurate statistics → better plans → potential better lock granularity

-- Update table statistics
ANALYZE accounts;

-- Check estimated vs. actual rows
EXPLAIN SELECT * FROM accounts WHERE balance > 100;

-- More accurate estimates allow planner to choose better scans
```

---

## Monitoring and Profiling

### PostgreSQL Logs

**Enable detailed logging**:
```ini
# postgresql.conf
log_statement = 'all'  # Log all statements
log_duration = on      # Log execution time
log_min_duration_statement = 1000  # Log if > 1 second
log_line_prefix = '%t [%p] %d %u %a '

# Look for SERIALIZATION_FAILURE in logs
```

### Identifying Bottlenecks

**High serialization failure rate**:
```sql
-- Check if retries happening
SELECT COUNT(*), error
FROM logs
WHERE error LIKE '%SERIALIZATION%'
GROUP BY extract(date FROM timestamp), error;

-- If > 1-2%, investigate:
-- 1. Concurrent transactions overlapping same tables
-- 2. Fine-grained locks being promoted excessively
-- 3. Dangerous structure patterns in your data access
```

**Lock memory exhaustion**:
```sql
-- Check current lock usage
SELECT COUNT(*) as predicate_lock_count
FROM pg_locks WHERE locktype = 'predicate';

-- Check if approaching limit
SELECT current_setting('max_predicate_locks')::int as limit,
       COUNT(*) as used
FROM pg_locks WHERE locktype = 'predicate'
GROUP BY 1;

-- If used > limit * 0.8, tune parameters
```

**Lock promotion frequency**:
```c
// Enable debug logging in PostgreSQL source
// #define PREDICATE_LOCK_DEBUG

// Recompile PostgreSQL
// watch for "Predicate lock: promoted" messages

// Count promotions
grep "promoted" postgresql.log | wc -l
```

---

## Workload-Specific Tuning

### OLTP Workload (High Concurrency)

**Characteristics**:
- Many short transactions
- Light, focused data access
- Many concurrent transactions competing

**Tuning Strategy**:
```ini
max_connections = 500
max_predicate_locks_per_transaction = 128  # Moderate locks
max_predicate_locks = 32000  # Enough for all txns
serializable_buffers = 64
```

**Query patterns**:
```sql
-- Short lookups
SELECT * FROM users WHERE id = 123;

-- Small batch inserts
INSERT INTO orders VALUES (...);
INSERT INTO orders VALUES (...);

-- Avoid full table scans
```

### OLAP Workload (Complex Queries)

**Characteristics**:
- Fewer, longer transactions
- Complex joins, many tables
- Aggregations and analytics

**Tuning Strategy**:
```ini
max_connections = 50
max_predicate_locks_per_transaction = 512  # More locks
max_predicate_locks = 25600  # Fewer total, but more per txn
serializable_buffers = 256  # Larger SLRU
```

**Query patterns**:
```sql
-- Large analytical queries
SELECT d.dept, COUNT(*) as emp_count
FROM employees e
JOIN departments d ON e.dept_id = d.id
GROUP BY d.dept;

-- Multi-table joins
SELECT *
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
WHERE o.order_date > '2024-01-01';
```

### Mixed Workload

**Characteristics**:
- Both OLTP and OLAP
- Variable transaction duration
- Competing for resources

**Tuning Strategy**:
```ini
max_connections = 200
max_predicate_locks_per_transaction = 256
max_predicate_locks = 50000
serializable_buffers = 128

# Use explicit isolation levels in application
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;  # For critical OLTP
SET TRANSACTION ISOLATION LEVEL READ COMMITTED; # For analytics
```

---

## Memory Pressure Mitigation

### Lock Promotion Strategy

**Default behavior**: Automatic promotion when limits approached

```
When lock count > threshold:
├─ Option 1: Promote to coarser granularity
│   ├─ Tuple locks → Page locks
│   └─ Page locks → Relation locks
├─ Option 2: Abort transaction
│   └─ Serialization failure
└─ Option 3: Defer lock acquisition
    └─ Wait for cleanup sweep
```

**Configuration approach**:

```c
// In predicate.c (internal constants)
#define COALESCING_THRESHOLD 0.75  // Promote at 75% of limit
#define ABORT_THRESHOLD 0.95       // Abort at 95% of limit

// Tuned at runtime via:
// 1. max_predicate_locks
// 2. max_predicate_locks_per_transaction
// 3. max_predicate_locks_per_relation
```

### Cleanup Scheduling

**Triggered by**:
1. SetNewSxactGlobalXmin() - every new serializable transaction
2. Per-transaction limit exceeded - aggressive cleanup
3. Global limit exceeded - immediate cleanup

**Effectiveness**:
```
Early cleanup: Releases locks from finished transactions early
              Keeps memory pressure manageable
              Small overhead

Late cleanup:  Waits until transactions fully completed
              Higher memory pressure initially
              May trigger aggressive promotion

Optimal: ClearOldPredicateLocks() called frequently
         Steady-state lock memory stays well below limit
```

---

## Connection Pooling Implications

### Backend Reuse

```sql
-- With connection pooling (e.g., PgBouncer):
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- Connection backend is reused
-- Local predicate lock hash is recreated
-- No impact on performance

-- But:
-- Pool should use 'session' or 'transaction' mode
-- Not 'statement' mode (which loses isolation level)
```

### Pooler Configuration

```ini
# pgbouncer.ini
[databases]
db = host=localhost port=5432 dbname=db

[pgbouncer]
pool_mode = session  # or transaction
max_client_conn = 1000
default_pool_size = 25
reserve_pool_size = 5
reserve_pool_timeout = 3
```

---

## Benchmarking SSI

### TPC-C Benchmark

```bash
# Run with SERIALIZABLE isolation
pgbench -i -s 100 db
pgbench -c 50 -j 8 -T 300 \
    -f ./serializable_test.sql db

# serializable_test.sql:
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- ... TPC-C transactions ...
COMMIT;
```

### Custom Workload Test

```sql
-- Test 1: Light contention
CREATE TABLE test AS
SELECT i as id, i * 10 as value FROM generate_series(1, 10000) i;

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM test WHERE value > 50000;
UPDATE test SET value = value + 1 WHERE id IN (100, 200, 300);
COMMIT;

-- Test 2: Heavy contention (same rows)
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM test WHERE value > 50000;
UPDATE test SET value = value + 1 WHERE id IN (1, 2, 3);  -- Always same rows
COMMIT;

-- Measure throughput, latency, retry rate
```

