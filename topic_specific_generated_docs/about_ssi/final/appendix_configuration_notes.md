# Appendix D: Configuration Notes

**GUC parameters, deployment recommendations, and tuning guidance for SSI.**

---

## Critical Configuration Parameters

### `max_predicate_locks`

```
Default: 262144 (256K locks)
Unit: number of lock entries
Type: integer
Minimum: 10
Maximum: 2147483647 (2^31 - 1)
```

**Purpose**: Total number of predicate locks available system-wide.

**Impact**:
- **Too Low** (e.g., 1000): Excessive lock coalescing, high false positive conflicts
- **Too High** (e.g., 10M): Wasteful SHMEM allocation, slower coalescing decisions

**Tuning Guidance**:
```
High-concurrency OLTP (100+ connections):
  max_predicate_locks = 500000 (500K)
  Reasoning: More concurrent locks needed, reduce coalescing pressure

Mixed OLTP/OLAP (moderate write rate):
  max_predicate_locks = 262144 (default)
  Reasoning: Balanced for typical workloads

Memory-constrained systems:
  max_predicate_locks = 65536 (64K)
  Reasoning: Limited SHMEM, more coalescing acceptable

Data warehouse (high scan rate, few writers):
  max_predicate_locks = 1000000 (1M)
  Reasoning: Large tables, many pages scanned, need fine granularity
```

**Formula** (estimate):
```
max_predicate_locks ≈ (num_connections × avg_locks_per_txn) × safety_factor

Typical values:
  avg_locks_per_txn = 10-100 (small txn) to 1000+ (large scans)
  safety_factor = 2-3 (headroom for peaks)
  
Example:
  100 connections
  avg_locks_per_txn = 50 (moderate workload)
  safety_factor = 2
  → max_predicate_locks ≈ 100 × 50 × 2 = 10,000 (minimum)
  → Recommended: 100,000-262,144 (depending on budget)
```

**Monitoring**:
```sql
-- Check predicate lock usage
SELECT
  sum(locks_granted)::int as total_locks,
  pg_size_pretty(sum(locks_granted) * 40 || ' bytes')::text as approx_memory
FROM pg_stat_activity
WHERE state = 'active' AND backend_xmin IS NOT NULL;

-- If approaching max_predicate_locks, increase it
-- If far below, consider decreasing to free SHMEM
```

---

### `max_predicate_locks_per_transaction`

```
Default: 64
Unit: number of lock entries per transaction
Type: integer
Minimum: 10
Maximum: 262144
```

**Purpose**: Limit predicate locks acquired by any single transaction (prevents runaway txns).

**Impact**:
- **Too Low** (e.g., 10): Small transactions hit limit, forced coalescing
- **Too High** (e.g., 10000): One txn can consume resources, starving others

**Tuning Guidance**:
```
Typical OLTP transactions (10-20 rows touched):
  max_predicate_locks_per_transaction = 64 (default)
  
Large scan operations (complex queries, many tables):
  max_predicate_locks_per_transaction = 256-512
  
Batch operations (millions of rows):
  max_predicate_locks_per_transaction = 1024+
  
Note: Batch operations may need SERIALIZABLE DEFERRABLE
  with higher timeout to avoid conflicts
```

**Trade-off Analysis**:
```
Low value (64):
  ✓ Prevents runaway transactions
  ✓ Encourages transaction splitting
  ✗ Forces coalescing on large queries
  ✗ May increase false positive conflicts

High value (1024):
  ✓ Allows large complex queries
  ✓ Finer lock granularity possible
  ✗ One transaction can consume SHMEM
  ✗ Delays coalescing (more locks held)
```

---

### `max_predicate_locks_per_relation` (PostgreSQL 9.2+)

```
Default: -1 (unlimited)
Unit: number of lock entries per relation
Type: integer
Minimum: -1 (unlimited) or 10
```

**Purpose**: Limit locks per table (forces table-level lock when exceeded).

**Impact**:
- **-1 (default)**: No per-table limit, fine granularity possible
- **Positive value**: Automatic table-level lock at threshold

**Tuning Guidance**:
```
Typical setting: Leave at -1 (default)
  Reasoning: Let system decide coalescing via global limits

Special case (very large table with many tuples):
  max_predicate_locks_per_relation = 10000
  Reasoning: Force coalescing at table level rather than system level
  
Special case (many small tables):
  max_predicate_locks_per_relation = 100
  Reasoning: Quickly coalesce to table level, reduce lock entries
```

---

## Deployment Recommendations

### Development/Testing
```sql
-- Conservative settings for catching issues
max_predicate_locks = 262144
max_predicate_locks_per_transaction = 64
max_predicate_locks_per_relation = -1

-- Rationale: Detect coalescing scenarios, exercise dangerous structure detection
```

### Production OLTP (High Concurrency)
```sql
-- High-concurrency settings for transactional workloads
max_predicate_locks = 1000000
max_predicate_locks_per_transaction = 256
max_predicate_locks_per_relation = -1

-- Rationale:
-- - High max_predicate_locks: many concurrent txns
-- - Increased per-txn limit: allow complex queries
-- - Unlimited per-relation: fine lock granularity
```

### Production Mixed Workload
```sql
-- Balanced for OLTP + analytics
max_predicate_locks = 500000
max_predicate_locks_per_transaction = 128
max_predicate_locks_per_relation = 50000

-- Rationale:
-- - Moderate limits: balance between flexibility and predictability
-- - Per-relation limit: automatic coalescing at table level
```

### Production OLAP/Data Warehouse
```sql
-- Specialized for analytics workloads
max_predicate_locks = 2000000
max_predicate_locks_per_transaction = 1024
max_predicate_locks_per_relation = -1

-- Rationale:
-- - Very high limits: large table scans need many locks
-- - Per-txn allows complex queries
-- - Unlimited per-relation: fine granularity for data quality checks
```

### Memory-Constrained Systems
```sql
-- Conservative settings for limited SHMEM
max_predicate_locks = 65536
max_predicate_locks_per_transaction = 32
max_predicate_locks_per_relation = 10000

-- Rationale:
-- - Low limits: reduce SHMEM footprint
-- - Aggressive per-relation coalescing: save memory
```

---

## Monitoring and Troubleshooting

### Check Current Settings
```sql
SHOW max_predicate_locks;
SHOW max_predicate_locks_per_transaction;
SHOW max_predicate_locks_per_relation;
```

### Detect Excessive Coalescing
```sql
-- If seeing many serialization failures with high contention:
-- Check if coalescing is too aggressive

-- Monitor query plans:
EXPLAIN ANALYZE SELECT ...;  -- Look for many table-level locks

-- Monitor system:
SELECT
  datname,
  numbackends,
  pg_stat_reset()  -- to get fresh stats
FROM pg_stat_database;
```

### Serialization Failure Analysis
```sql
-- If seeing SQLSTATE 40001 frequently:

-- Option 1: Increase max_predicate_locks
ALTER SYSTEM SET max_predicate_locks = 500000;

-- Option 2: Check for long-running transactions (preventing safe snapshots)
SELECT pid, usename, state, xact_start
FROM pg_stat_activity
WHERE xact_start < now() - INTERVAL '5 minutes';

-- Option 3: Check for patterns of dangerous structures
-- (requires application logging or trace)
```

### SHMEM Allocation
```sql
-- Check how much SHMEM SSI is using
-- (Estimated as: max_predicate_locks × 40 bytes + overhead)

SELECT
  setting,
  CAST(setting AS bigint) * 40 / 1024 / 1024 as "SHMEM (MB)"
FROM pg_settings
WHERE name = 'max_predicate_locks';
```

---

## Performance Tuning

### Reducing Serialization Failures

```sql
-- Strategy 1: Increase max_predicate_locks
ALTER SYSTEM SET max_predicate_locks = 1000000;
SELECT pg_ctl_reload_conf();  -- Reload

-- Strategy 2: Use DEFERRABLE for read-only transactions
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE DEFERRABLE;
  SELECT ... FROM large_table;
COMMIT;
-- Eliminates conflicts for RO txns

-- Strategy 3: Batch smaller transactions
-- Instead of: BEGIN; for many rows, do
BEGIN;
  INSERT INTO table SELECT ... FROM source LIMIT 10000;
COMMIT;
BEGIN;
  INSERT INTO table SELECT ... FROM source LIMIT 10000;
COMMIT;
-- Reduces lock accumulation per txn

-- Strategy 4: Reduce transaction duration
-- Shorter txns = fewer conflicts = lower failure rate
BEGIN;
  SELECT data INTO var FROM table WHERE id = 1;
  -- ... business logic ...
COMMIT;
-- vs.
BEGIN;
  SELECT data INTO var FROM table WHERE id = 1;
  -- ... 10 seconds of business logic ...
COMMIT;
```

### Reducing False Positive Conflicts

```sql
-- Strategy 1: Use finer transaction isolation for RO operations
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;  -- No conflicts
  SELECT ... FROM table;
COMMIT;

-- Strategy 2: Split large scans
-- If transaction needs: SELECT ... (entire table)
-- Consider:
BEGIN;
  SELECT ... FROM table WHERE id BETWEEN 1 AND 1000000;
  -- Process...
COMMIT;
BEGIN;
  SELECT ... FROM table WHERE id BETWEEN 1000001 AND 2000000;
  -- Process...
COMMIT;
-- Reduces lock accumulation

-- Strategy 3: Use ORDER BY to enable early termination
SELECT ... FROM large_table
WHERE condition IS TRUE
ORDER BY id
LIMIT 1000;  -- Process in batches
```

### Monitoring in Production

```bash
# Monitor for serialization failures
tail -f /var/log/postgresql/postgresql.log | \
  grep "SERIALIZATION_FAILURE\|40001"

# Check current lock usage
psql -c "SELECT pid, locks_granted FROM pg_stat_activity WHERE state = 'active';"

# Alert if approaching limit
psql -c "
SELECT COUNT(*) as active_locks
FROM pg_stat_activity
WHERE state = 'active' AND backend_xmin IS NOT NULL;
" | awk '{if ($1 > 200000) print "WARNING: Near max_predicate_locks limit"}'
```

---

## Known Limitations

### Memory Bounded
- SSI requires bounded memory for predictability
- If transaction generates >max_predicate_locks locks, it must coalesce (lose granularity)
- Very large transactions (scanning >100M rows) may hit this limit

### Not Applicable To
- Temporary tables
- Unlogged tables (partial support)
- Foreign tables (no SSI support)

### Interaction With Other Features
- **Parallel queries**: Predicate locks must be managed carefully
- **Connection pooling**: Each pool participant sees separate SERIALIZABLEXACT
- **Replication**: Standby replicas don't participate in SSI

---

## Upgrading PostgreSQL Versions

### PostgreSQL 9.0 → 9.1+ (First SSI Implementation)
- Configuration: New parameters introduced
- Impact: Default behavior changes to SERIALIZABLE
- Action: Set isolation level explicitly in applications

### PostgreSQL 9.1 → 9.2+ (Per-Relation Limits)
- New parameter: `max_predicate_locks_per_relation`
- Benefit: More granular control
- Action: Update tuning if needed

### PostgreSQL 13+ (Parallel Worker Support)
- Enhancement: Better predicate lock sharing in parallel queries
- Benefit: Improved performance for complex queries
- Action: No configuration changes needed

---

## Related Resources

- [Configuration Notes](#) - This appendix
- [Performance and Tuning](11_performance_and_tuning.md) - Workload-specific guidance
- [Case Studies](17_case_studies.md) - Real-world tuning examples
- [Error Modes and Retries](12_error_modes_and_retries.md) - Handling serialization failures
