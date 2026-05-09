# PostgreSQL SSI: Error Modes, Retry Semantics, and Hooks

## Serialization Failure Semantics

### Error Definition

**SQLState**: 40001
**Error Class**: SERIALIZATION_FAILURE
**PostgreSQL Name**: ERRCODE_SERIALIZATION_FAILURE
**Standard SQL**: Serialization failure

```c
// In src/backend/utils/errcodes.txt
40001	ERRCODE_SERIALIZATION_FAILURE	serialization_failure

// When thrown
ereport(ERROR,
    (errcode(ERRCODE_SERIALIZATION_FAILURE),
     errmsg("could not serialize access due to concurrent update")));
```

### When Serialization Failure Occurs

**Case 1: At commit time** (most common)
```
PreCommit_CheckForSerializationFailure() detects dangerous structure
→ ereport(ERROR, SERIALIZATION_FAILURE)
→ Transaction ROLLBACK initiated
→ Control returns to client
```

**Case 2: During execution** (when marked DOOMED)
```
OnConflict_CheckForSerializationFailure() detects cycle
→ MarkSxactDoomed(txn)
→ On next query executor cycle:
  → CHECK InterruptPending
  → ProcessInterrupts()
  → ereport(ERROR, SERIALIZATION_FAILURE)
→ Transaction ROLLBACK
```

**Case 3: During garbage collection** (rare)
```
ClearOldPredicateLocks() detects live transaction must abort
→ Very rare, usually caught earlier
```

### Client Visibility

**libpq / psycopg2 / other drivers**:
```python
try:
    with connection.cursor() as cur:
        cur.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        # ... transaction ...
        connection.commit()
except SerializationFailureException as e:
    # Catch SQLSTATE 40001
    # Retry transaction
    connection.rollback()
```

**SQL Level**:
```sql
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- ERROR: could not serialize access due to concurrent update
-- (SQLSTATE: 40001)
```

---

## Retry Strategies

### Basic Retry Pattern

```python
# Pattern 1: Simple retry with fixed delay
MAX_RETRIES = 3
for attempt in range(MAX_RETRIES):
    try:
        with connection.transaction(isolation=SERIALIZABLE):
            result = execute_business_logic()
            return result
    except SerializationFailure:
        if attempt == MAX_RETRIES - 1:
            raise  # Give up after max retries
        time.sleep(0.1)  # Fixed 100ms delay

# Pattern 2: Exponential backoff
MAX_RETRIES = 5
INITIAL_DELAY = 0.01  # 10ms
for attempt in range(MAX_RETRIES):
    try:
        with connection.transaction(isolation=SERIALIZABLE):
            return execute_business_logic()
    except SerializationFailure:
        if attempt == MAX_RETRIES - 1:
            raise
        delay = INITIAL_DELAY * (2 ** attempt)
        jitter = random.uniform(0, delay * 0.1)
        time.sleep(delay + jitter)

# Pattern 3: Timeout-based retry
TIMEOUT = 5.0  # 5 seconds total
START = time.time()
while time.time() - START < TIMEOUT:
    try:
        with connection.transaction(isolation=SERIALIZABLE):
            return execute_business_logic()
    except SerializationFailure:
        pass  # Retry immediately
raise TimeoutError("Could not complete transaction")
```

### Application-Level Implementations

**ORMs often provide built-in retry logic**:

```python
# SQLAlchemy example
from sqlalchemy.orm import Session
from sqlalchemy.pool import QueuePool

session = Session(
    bind=engine,
    # Automatic retry on serialization failure
    execution_options={"isolation_level": "SERIALIZABLE"}
)

# With retry decorator
@retry(exception=SerializationFailure, max_attempts=3, backoff=exponential)
def transactional_operation():
    # Automatically retried on serialization failure
    pass
```

### Distributed Transaction Retry

**For multi-database transactions**:

```python
def distributed_transaction_with_retry():
    for attempt in range(MAX_RETRIES):
        try:
            # Begin transaction on all databases
            conn_pg.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            conn_mysql.execute("START TRANSACTION")
            
            # Execute operations
            work_result = execute_work_across_databases()
            
            # Commit all
            conn_pg.commit()
            conn_mysql.commit()
            return work_result
            
        except SerializationFailure:
            # Rollback all
            conn_pg.rollback()
            conn_mysql.rollback()
            
            if attempt == MAX_RETRIES - 1:
                raise
            # Retry...
```

---

## Guarantees and Anomalies Prevented

### Anomaly Definitions

**Dirty Read** (prevented by all isolation levels):
```
T1 writes X
T2 reads X (before T1 commits)
T1 aborts
→ T2 has seen value that never existed
```

**Non-repeatable Read** (prevented by REPEATABLE READ and SERIALIZABLE):
```
T1 reads X = 100
T2 writes X = 200, commits
T1 reads X = 200
→ Same value read twice, different results
```

**Phantom Read** (prevented by SERIALIZABLE):
```
T1 reads rows matching predicate P
T2 inserts/deletes row matching P, commits
T1 reads rows matching predicate P again
→ Different set of rows
```

**Serialization Anomaly** (prevented by SERIALIZABLE only):
```
T1: Read  X, Read Y, Write  Z=X+Y
T2: Read  Z, Write X=Z+100
T3: Read  X, Write Y=X+200

Possible interleaving:
├─ T1 reads X=10, Y=20
├─ T2 reads Z (waits)
├─ T3 reads X=10
├─ T1 computes Z=30, writes
├─ T2 reads Z=30, writes X=130
├─ T3 writes Y=330
├─ T1 committed: X=130, Y=330, Z=30 ← Inconsistent!
│   (should be X+Y=Z, but 130+330≠30)

With SERIALIZABLE:
└─ One transaction aborts, result is consistent
```

### SSI's Isolation Guarantee

```
Property: All serializable transactions at isolation level SERIALIZABLE
          will see consistent data results equivalent to some serial
          (one-at-a-time) execution of those transactions.

Proof basis: 
├─ Detects all cycles containing dangerous structure
├─ By theorem (Cahill et al.), all isolation anomalies contain
│   at least one dangerous structure in their cycle
└─ Therefore no anomalies can occur
```

---

## Hooks and Extension Points

### Module Hook Types

**Predicate lock hooks** (in predicate.c):

```c
// Hook called when predicate lock is acquired
typedef void (*PredicateLock_LockAcquiredHook_type)(
    PREDICATELOCKTARGETTAG *tag,
    SERIALIZABLEXACT *sxact,
    bool isNewTarget);

// Hook called when transaction starts
typedef void (*PredicateLock_TransactionStartHook_type)(
    SERIALIZABLEXACT *sxact);

// Hook called when transaction commits
typedef void (*PredicateLock_TransactionCommitHook_type)(
    SERIALIZABLEXACT *sxact,
    bool isCommit);

// Hook called when dangerous structure detected
typedef void (*PredicateLock_DangerousStructureHook_type)(
    const SERIALIZABLEXACT *tin,
    const SERIALIZABLEXACT *tpivot,
    const SERIALIZABLEXACT *tout);
```

### Hook Registration

```c
// In module initialization
void my_module_init(void) {
    // Register hooks
    PredicateLock_LockAcquiredHook = my_lock_acquired_hook;
    PredicateLock_DangerousStructureHook = my_dangerous_structure_hook;
}

void my_lock_acquired_hook(
    PREDICATELOCKTARGETTAG *tag,
    SERIALIZABLEXACT *sxact,
    bool isNewTarget) {
    // Custom logic
    // E.g., log to external system
    // E.g., update statistics
    // E.g., trigger application logic
}
```

### Example Extension: Conflict Logging

```c
// Extension to log all conflicts for analysis

void conflict_logger_init(void) {
    PredicateLock_DangerousStructureHook = log_dangerous_structure;
}

void log_dangerous_structure(
    const SERIALIZABLEXACT *tin,
    const SERIALIZABLEXACT *tpivot,
    const SERIALIZABLEXACT *tout) {
    
    // Create log entry
    INSERT INTO ssi_conflicts_log
    VALUES (tin->topXid, tpivot->topXid, tout->topXid,
            current_timestamp, 'DANGEROUS_STRUCTURE_DETECTED');
    
    // Analyze patterns for application tuning
}

// Query conflicts afterward
SELECT * FROM ssi_conflicts_log
WHERE timestamp > now() - interval '1 hour'
ORDER BY timestamp DESC
LIMIT 100;
```

### Example Extension: Custom Conflict Resolution

```c
// Alternative to aborting: custom resolution strategy

void custom_conflict_resolution_init(void) {
    PredicateLock_DangerousStructureHook = 
        resolve_conflict_application_specific;
}

void resolve_conflict_application_specific(
    const SERIALIZABLEXACT *tin,
    const SERIALIZABLEXACT *tpivot,
    const SERIALIZABLEXACT *tout) {
    
    // Query application context
    priority_tin = get_transaction_priority(tin->topXid);
    priority_tpivot = get_transaction_priority(tpivot->topXid);
    priority_tout = get_transaction_priority(tout->topXid);
    
    // Abort lowest priority instead of default strategy
    if (priority_tout < priority_tpivot && 
        priority_tout < priority_tin) {
        MarkSxactDoomed((SERIALIZABLEXACT *)tout);
    } else if (priority_tpivot < priority_tin) {
        MarkSxactDoomed((SERIALIZABLEXACT *)tpivot);
    } else {
        MarkSxactDoomed((SERIALIZABLEXACT *)tin);
    }
}
```

---

## Idempotency and Restart Semantics

### Transaction Idempotency

**Requirement for safe retry**:
```
Idempotent operation: f(f(x)) = f(x)

Examples:
✓ SET status = 'active'      (idempotent)
✗ UPDATE balance SET balance = balance + 100  (NOT idempotent)
✓ INSERT INTO set SELECT ... WHERE NOT EXISTS (idempotent with dedup)
✗ INSERT INTO list VALUES (...)   (NOT idempotent)
✗ DELETE FROM table LIMIT 1        (NOT idempotent)
```

**Safe retry pattern**:
```python
def transfer_funds_safe(from_account, to_account, amount):
    """Idempotent transfer using transfer_id"""
    
    while True:
        try:
            with connection.transaction(
                isolation=SERIALIZABLE):
                
                # Use idempotency key
                INSERT INTO transfers (transfer_id, from_id, to_id, amount)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (transfer_id) DO NOTHING;
                
                # Only update if not yet processed
                UPDATE accounts
                SET balance = balance - ?
                WHERE id = ? AND NOT processed_transfer(?)
                
                UPDATE accounts
                SET balance = balance + ?
                WHERE id = ? AND NOT processed_transfer(?)
                
                return True
        except SerializationFailure:
            # Safe to retry: idempotent operations
            pass
```

### Event Sourcing Pattern (SSI-friendly)

```python
# SSI works well with event sourcing

def append_event_safe(aggregate_id, event_type, event_data):
    """Append-only event log prevents retryability issues"""
    
    while True:
        try:
            with connection.transaction(isolation=SERIALIZABLE):
                
                # Get aggregate version (read-your-writes)
                current_version = (
                    SELECT MAX(version)
                    FROM events
                    WHERE aggregate_id = ?
                )
                
                # Append new event
                INSERT INTO events 
                VALUES (?, ?, ?, ?, current_version + 1)
                
                # Idempotent: append is by nature idempotent
                return True
                
        except SerializationFailure:
            # Safe to retry: appending is idempotent
            pass
```

---

## Interaction with Other Features

### Savepoint Behavior

```sql
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

SAVEPOINT sp1;
    SELECT * FROM accounts;  -- Predicate locks acquired
    UPDATE accounts SET balance = balance - 100;
SAVEPOINT sp2;
    -- Conflict detected here, transaction marked DOOMED
ROLLBACK TO sp2;
    -- Rollback to savepoint removes writes but NOT locks!
COMMIT;
    -- ERROR: Serialization failure at commit time
```

### Deferred Constraints

```sql
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

SET CONSTRAINTS ALL DEFERRED;

-- Operations allowed that would normally violate constraints
INSERT INTO t VALUES (1, 2);
DELETE FROM t WHERE id = 1;

-- Constraint check deferred to COMMIT
COMMIT;  -- May fail if constraint violations + conflicts detected
```

### Prepared Statements

```python
# Prepared statements work normally with SSI

stmt = connection.prepare(
    "SELECT balance FROM accounts WHERE id = $1")

while True:
    try:
        with connection.transaction(isolation=SERIALIZABLE):
            balance = stmt.execute(account_id)
            # ... more operations ...
            connection.commit()
            break
    except SerializationFailure:
        connection.rollback()
        # Retry - prepared statement remains valid
```

---

## Best Practices Summary

**Do**:
- Use SERIALIZABLE for transactions that modify shared state
- Implement application-level retry logic (with backoff)
- Keep transactions short and focused
- Use transactions that are idempotent when possible
- Monitor for high retry rates (indicates high contention)
- Use connection pooling for better resource utilization

**Don't**:
- Assume SERIALIZABLE guarantees no retries (it doesn't!)
- Use SERIALIZABLE everywhere (overhead unnecessary)
- Hold long transactions (increases contention)
- Call external services inside serializable transactions
- Assume deterministic behavior (retries may occur)

