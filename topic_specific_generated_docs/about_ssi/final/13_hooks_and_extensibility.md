# PostgreSQL SSI: Hooks and Extensibility

## SSI Extension Architecture

### Plugin Interface

PostgreSQL allows modules to extend SSI functionality through function pointer hooks and structured extension APIs. This enables:

1. **Custom monitoring systems** - Hook into conflict detection
2. **Alternative algorithms** - Implement different dangerous structure criteria
3. **Application-specific policies** - Custom transaction abort decisions
4. **Integration with external systems** - Conflict event streaming

### Module Loading

```sql
-- Load module implementing custom SSI logic
CREATE EXTENSION ssi_monitoring;

-- Verify loaded
SELECT installed_by('ssi_monitoring');

-- Module initialization called, hooks registered
```

---

## Hook Interface

### Available Hooks

**Hook 1: Transaction Registration**

```c
typedef void (*SerializableTransactionRegisterHook_type)(
    SERIALIZABLEXACT *sxact,
    TransactionId xid);

SerializableTransactionRegisterHook_type 
    SerializableTransactionRegisterHook = NULL;

// Called when:
// - GetSerializableTransactionSnapshot() allocates SERIALIZABLEXACT
// - RegisterPredicateLockingXid() is called
// - XID assigned to transaction
```

**Hook 2: Lock Acquisition**

```c
typedef void (*PredicateLockAcquiredHook_type)(
    const PREDICATELOCKTARGETTAG *tag,
    const SERIALIZABLEXACT *sxact);

PredicateLockAcquiredHook_type 
    PredicateLockAcquiredHook = NULL;

// Called when:
// - PredicateLockAcquire() successfully creates lock
// - Lock added to target and transaction lists
// - Before returning to caller

// NOT called for:
// - Locks that were skipped (coarser lock exists)
// - Locks that were promoted
```

**Hook 3: Conflict Detection**

```c
typedef void (*RWConflictDetectedHook_type)(
    const SERIALIZABLEXACT *reader,
    const SERIALIZABLEXACT *writer,
    bool write_after_read);

RWConflictDetectedHook_type 
    RWConflictDetectedHook = NULL;

// Called when:
// - FlagRWConflict() creates new rw-conflict
// - Both transactions still active
// - read_after_write = (reader reads, then writer writes)

// Parameters:
// reader: Transaction that has conflict in
// writer: Transaction that has conflict out  
// write_after_read: Direction of conflict
```

**Hook 4: Dangerous Structure Detection**

```c
typedef void (*DangerousStructureDetectedHook_type)(
    const SERIALIZABLEXACT *tin,
    const SERIALIZABLEXACT *tpivot,
    const SERIALIZABLEXACT *tout,
    bool is_real_cycle);

DangerousStructureDetectedHook_type 
    DangerousStructureDetectedHook = NULL;

// Called when:
// - OnConflict_CheckForSerializationFailure() detects pattern
// - Pattern: Tin → Tpivot → Tout
// - is_real_cycle: TRUE if Tout has already committed

// Invoked BEFORE abort decision made
// Hook can influence decision via side effects
```

**Hook 5: Transaction Abort Decision**

```c
typedef bool (*ShouldAbortTransactionHook_type)(
    const SERIALIZABLEXACT *victim,
    const SERIALIZABLEXACT *conflicting);

ShouldAbortTransactionHook_type 
    ShouldAbortTransactionHook = NULL;

// Return value:
// TRUE: Abort victim (default PostgreSQL behavior)
// FALSE: Keep victim, abort conflicting instead

// Allows application-specific abort policies
// E.g., priority-based abort
```

**Hook 6: Transaction Cleanup**

```c
typedef void (*TransactionCleanupHook_type)(
    SERIALIZABLEXACT *sxact,
    bool was_committed);

TransactionCleanupHook_type 
    TransactionCleanupHook = NULL;

// Called when:
// - ReleaseOneSerializableXact() releases transaction
// - After predicate locks removed
// - Before SERIALIZABLEXACT deallocated

// Allows modules to perform cleanup
// E.g., persist conflict history to disk
```

---

## Extension Development Example

### Complete Monitoring Extension

**ssi_monitoring.c**:

```c
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "storage/predicate_internals.h"

PG_MODULE_MAGIC;

// Extension state
typedef struct {
    uint64 total_conflicts;
    uint64 total_dangerous_structures;
    uint64 total_aborts;
    uint64 ro_safe_transactions;
} SSIStats;

static SSIStats ssi_stats = {0};

// Hook implementations
static void
my_conflict_hook(const SERIALIZABLEXACT *reader,
                 const SERIALIZABLEXACT *writer,
                 bool write_after_read) {
    ssi_stats.total_conflicts++;
    
    // Log to pg_stat_statements-like view
    LOG_CONFLICT_TO_TABLE(reader->topXid, writer->topXid);
}

static void
my_dangerous_structure_hook(
    const SERIALIZABLEXACT *tin,
    const SERIALIZABLEXACT *tpivot,
    const SERIALIZABLEXACT *tout,
    bool is_real_cycle) {
    
    ssi_stats.total_dangerous_structures++;
    
    if (is_real_cycle) {
        ssi_stats.total_aborts++;
        LOG_ABORT_DECISION(tin, tpivot, tout);
    }
}

// Exported function: get statistics
PG_FUNCTION_INFO_V1(get_ssi_stats);

Datum
get_ssi_stats(PG_FUNCTION_ARGS) {
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum values[4];
    bool nulls[4] = {false};
    
    tupdesc = CreateTemplateTupleDesc(4);
    TupleDescInitEntry(tupdesc, 1, "total_conflicts", 
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 2, "total_dangerous_structures",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 3, "total_aborts",
                       INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 4, "ro_safe_transactions",
                       INT8OID, -1, 0);
    
    values[0] = Int64GetDatum(ssi_stats.total_conflicts);
    values[1] = Int64GetDatum(ssi_stats.total_dangerous_structures);
    values[2] = Int64GetDatum(ssi_stats.total_aborts);
    values[3] = Int64GetDatum(ssi_stats.ro_safe_transactions);
    
    tuple = heap_form_tuple(tupdesc, values, nulls);
    return HeapTupleGetDatum(tuple);
}

// Module initialization
void
_PG_init(void) {
    // Register our hooks
    RWConflictDetectedHook = my_conflict_hook;
    DangerousStructureDetectedHook = my_dangerous_structure_hook;
    
    // Create tracking table if not exists
    CREATE_SSI_CONFLICTS_TABLE();
}

void
_PG_fini(void) {
    // Cleanup hooks
    RWConflictDetectedHook = NULL;
    DangerousStructureDetectedHook = NULL;
}
```

**ssi_monitoring.sql**:

```sql
CREATE TABLE ssi_conflicts (
    recorded_at TIMESTAMP DEFAULT NOW(),
    reader_xid XID,
    writer_xid XID,
    pattern_type TEXT,
    victim_xid XID
);

CREATE TABLE ssi_stats (
    stat_name TEXT,
    stat_value BIGINT,
    recorded_at TIMESTAMP DEFAULT NOW()
);

CREATE FUNCTION get_ssi_stats()
RETURNS TABLE (
    total_conflicts BIGINT,
    total_dangerous_structures BIGINT,
    total_aborts BIGINT,
    ro_safe_transactions BIGINT
) AS 'ssi_monitoring' LANGUAGE C;

SELECT * FROM get_ssi_stats();
```

---

## Advanced Extension Patterns

### Pattern 1: Priority-Based Abort

**Priority override for abort decisions**:

```c
static bool
priority_based_abort_hook(
    const SERIALIZABLEXACT *victim,
    const SERIALIZABLEXACT *conflicting) {
    
    int victim_priority = get_transaction_priority(victim->topXid);
    int conflict_priority = get_transaction_priority(conflicting->topXid);
    
    // Abort lower priority transaction
    if (conflict_priority < victim_priority) {
        return false;  // Don't abort victim, abort conflicting
    }
    return true;  // Default: abort victim
}
```

**Integration**:
```c
void _PG_init(void) {
    ShouldAbortTransactionHook = priority_based_abort_hook;
}
```

### Pattern 2: Distributed Conflict Tracking

**Send conflicts to external monitoring system**:

```c
static void
external_conflict_tracker(
    const SERIALIZABLEXACT *reader,
    const SERIALIZABLEXACT *writer,
    bool write_after_read) {
    
    // Format conflict as JSON
    StringInfo json = makeStringInfo();
    appendStringInfo(json,
        "{\"reader\": %u, \"writer\": %u, \"timestamp\": \"%s\"}",
        reader->topXid, writer->topXid,
        timestamptz_to_str(GetCurrentTimestamp()));
    
    // Send to Kafka/message queue
    send_to_external_system(json->data);
}

void _PG_init(void) {
    RWConflictDetectedHook = external_conflict_tracker;
}
```

### Pattern 3: Conflict Suppression

**Suppress spurious conflicts for known patterns**:

```c
static void
suppress_false_conflicts(
    const SERIALIZABLEXACT *reader,
    const SERIALIZABLEXACT *writer,
    bool write_after_read) {
    
    // Check if this is a known false positive pattern
    if (is_application_generated_conflict(reader, writer)) {
        // Suppress conflict - don't flag it
        return;
    }
    
    // Otherwise proceed with default behavior
    RWConflictDetectedHook = NULL;  // Call original
    RWConflictDetectedHook(reader, writer, write_after_read);
}
```

---

## Constraints and Limitations

### What Hooks Can Do

✓ Log/monitor conflicts
✓ Update external statistics  
✓ Make abort decisions
✓ Record conflict history
✓ Interact with application context
✓ Update custom data structures

### What Hooks CANNOT Do

✗ Modify transaction state directly
✗ Release locks (SSI manages them)
✗ Create new predicate locks
✗ Perform transactions
✗ Use palloc without context
✗ Call callback functions recursively

### Lock Handling in Hooks

```c
// ✓ Allowed: Read locks (safe, won't trigger deadlock)
LWLockAcquire(lock, LW_SHARED);
read_data();
LWLockRelease(lock);

// ✗ Don't: Acquire exclusive locks
// Can cause deadlock with main SSI locking

// ✓ Better: Use spinlocks for brief critical sections
SpinLockAcquire(&my_lock);
update_my_data();
SpinLockRelease(&my_lock);

// ✗ Never: Call PG functions that might transaction
// (would deadlock with outer transaction)
```

### Memory Management in Hooks

```c
// ✓ Allocate in appropriate memory context
MemoryContext oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
data = palloc(size);
MemoryContextSwitchTo(oldcontext);

// ✗ Don't: Use CurrentMemoryContext (it's temporary)
// Will be freed after transaction

// ✗ Don't: Allocate unbounded memory
// Could exhaust server resources
```

---

## Hook Registration API

### Safe Hook Registration

```c
// Pattern: Check existing hook, chain if necessary
static void
my_conflict_hook(const SERIALIZABLEXACT *reader,
                 const SERIALIZABLEXACT *writer,
                 bool write_after_read) {
    
    // My processing...
    do_my_stuff();
    
    // Call chained hook if exists
    if (prev_conflict_hook)
        prev_conflict_hook(reader, writer, write_after_read);
}

void _PG_init(void) {
    prev_conflict_hook = RWConflictDetectedHook;
    RWConflictDetectedHook = my_conflict_hook;
}

void _PG_fini(void) {
    RWConflictDetectedHook = prev_conflict_hook;
}
```

---

## Custom Dangerous Structure Patterns

### Extension: Alternative Cycle Detection

```c
// Custom cycle detection (different than default)
static bool
custom_dangerous_structure_check(
    const SERIALIZABLEXACT *tin,
    const SERIALIZABLEXACT *tpivot) {
    
    // Custom algorithm: look for different pattern
    // E.g., require all transactions from same application
    
    if (get_application_id(tin) != 
        get_application_id(tpivot)) {
        return false;  // Not a dangerous structure
    }
    
    return true;  // Custom rule applies
}

void _PG_init(void) {
    // Register hook - gets called for EVERY pattern detected
    // Can suppress or enhance detection
    DangerousStructureDetectedHook = 
        my_custom_detection_hook;
}
```

---

## Stability and Compatibility

### Version Checking

```c
#if PG_VERSION_NUM >= 130000
    // Use PostgreSQL 13+ APIs
    #define USE_NEW_API 1
#else
    // Fall back to older APIs
    #define USE_OLD_API 1
#endif
```

### Hook API Stability

**PostgreSQL Compatibility**:
- Hooks added in v9.1 (when SSI added)
- Stable across major versions (usually)
- Carefully versioned in PG headers

**Extension Best Practices**:
```sql
-- Declare compatible versions
CREATE EXTENSION ssi_monitoring VERSION '1.0';

-- In control file
# ssi_monitoring.control
comment = 'SSI Monitoring Extension'
default_version = '1.0'
requires = 'postgres >= 9.1'
```

### Performance Considerations

**Hook call overhead**:
```
Per-conflict: ~0.1-1µs (function pointer + args)
Per-dangerous-structure: ~0.5-5µs
Per-lock: ~0.1-0.5µs

Total impact: 1-10% overhead if hooks active
```

**Optimization**:
```c
// Only call hook if really needed
if (UNLIKELY(RWConflictDetectedHook != NULL)) {
    RWConflictDetectedHook(reader, writer, write_after_read);
}
```

---

## Production Deployment

### Extension Deployment Checklist

- [ ] Compile and test with current PostgreSQL version
- [ ] Test on non-production first
- [ ] Verify performance impact < acceptable threshold
- [ ] Configure to auto-load in postgresql.conf
- [ ] Set up monitoring for hook exceptions
- [ ] Test failover/restart behavior
- [ ] Document hook behavior for ops team

### Configuration

```ini
# postgresql.conf
shared_preload_libraries = 'ssi_monitoring'

# Or load per-session
psql -c "LOAD 'ssi_monitoring'" database
```

### Troubleshooting

```sql
-- Check if extension loaded
SELECT * FROM pg_extension WHERE extname = 'ssi_monitoring';

-- Check if hooks active (via monitoring)
SELECT * FROM get_ssi_stats();

-- Check for hook errors in logs
SELECT * FROM pg_log WHERE message LIKE '%ssi_monitoring%';
```


---

## Prerequisites
- Complete understanding of all prior chapters (especially Chapter 12)
- Familiarity with PostgreSQL transaction isolation and MVCC
- Understanding of shared memory and LWLock synchronization

## Next Steps
→ [Back to Architecture Overview](02_architecture_overview.md)
→ [Jump to Deep Dives](18_deep_dives.md) for advanced topics
