# Case Studies: Real-World SSI Conflict Patterns

This chapter presents real-world scenarios demonstrating SSI's dangerous structure detection, serialization failures, retry patterns, and resolution strategies.

## Case Study 1: Inventory Update Race Condition

### Scenario
E-commerce system: checking inventory before purchase, with concurrent updates.

### Setup
```sql
CREATE TABLE inventory (
    product_id INT PRIMARY KEY,
    quantity INT,
    last_updated TIMESTAMP
);

INSERT INTO inventory VALUES (1, 100, NOW());
```

### Concurrent Transactions

```
T1: Customer A (Serializable)
    BEGIN ISOLATION LEVEL SERIALIZABLE DEFERRABLE;
    SELECT quantity FROM inventory WHERE product_id = 1;
    -- Reads: quantity = 100
    
T2: Restocking (Serializable)
    BEGIN ISOLATION LEVEL SERIALIZABLE;
    UPDATE inventory SET quantity = 150 WHERE product_id = 1;
    COMMIT;
    
T3: Customer B (Serializable)
    BEGIN ISOLATION LEVEL SERIALIZABLE;
    SELECT quantity FROM inventory WHERE product_id = 1;
    -- T2 already committed, sees: quantity = 150
    UPDATE inventory SET quantity = 150 - 20 WHERE product_id = 1;
    COMMIT;

T1: (continuing after T3 commits)
    UPDATE inventory SET quantity = 100 - 10 WHERE product_id = 1;
    COMMIT;  -- ERROR: SERIALIZATION_FAILURE
    --> When rolled back and retried, will succeed with quantity = 120
```

### Dangerous Structure Detected
- **Tin (T2)**: Writes to inventory (restocking)
- **Tpivot (T1)**: Reads from Tin's write (sees restocking)
- **Tout (T3)**: Writes after T1's read (updates inventory again)

Pattern: T2 writes → T1 reads → T3 writes → but T1 needs to read T2's value still visible

### Why It's Dangerous
If all three committed in serial order (T2 → T3 → T1), the final quantity would be:
- After T2: 150
- After T3: 130 (150 - 20)
- After T1: 120 (150 - 30), **not** the value it expected (100 - 10)

### Solution: Application Retry Pattern
```python
MAX_RETRIES = 5
retry_count = 0

while retry_count < MAX_RETRIES:
    try:
        conn.set_isolation_level(SERIALIZABLE)
        cursor = conn.cursor()
        
        # Read
        cursor.execute("SELECT quantity FROM inventory WHERE product_id = %s", (1,))
        current_qty = cursor.fetchone()[0]
        
        # Check business logic
        if current_qty < 10:
            conn.rollback()
            raise InsufficientInventoryError()
        
        # Write
        cursor.execute(
            "UPDATE inventory SET quantity = quantity - %s WHERE product_id = %s",
            (10, 1)
        )
        
        conn.commit()
        break  # Success
        
    except psycopg2.OperationalError as e:
        if "40001" in str(e):  # SERIALIZATION_FAILURE
            conn.rollback()
            retry_count += 1
            if retry_count >= MAX_RETRIES:
                raise
            time.sleep(0.01 * retry_count)  # Exponential backoff
        else:
            raise
```

### Key Takeaway
- **Idempotency**: The retry must reach the same conclusion as original attempt
- **Backoff Strategy**: Exponential backoff reduces thundering herd
- **Max Retries**: Prevent infinite loops (use circuit breaker for persistent failures)

---

## Case Study 2: Bank Transfer Anomaly

### Scenario
Classic bank transfer problem where naive SI would allow violations.

### Setup
```sql
CREATE TABLE accounts (
    account_id INT PRIMARY KEY,
    balance DECIMAL,
    version INT DEFAULT 0
);

INSERT INTO accounts VALUES (1, 1000, 0), (2, 1000, 0);
```

### Non-Serializable Anomaly (with SI, no SSI)
```
T1 (SI): SELECT SUM(balance) FROM accounts;  -- reads 2000
T2 (SI): UPDATE accounts SET balance -= 100 WHERE account_id = 1;
         UPDATE accounts SET balance += 100 WHERE account_id = 2;
         COMMIT;
T3 (SI): SELECT SUM(balance) FROM accounts;  -- reads 2100 (?!)
         -- Sees only T2's INSERT, not both updates

T1:      SELECT SUM(balance) FROM accounts;  -- COMMIT at 2000
         -- Sees neither T2's write nor T3's (if it existed)
```

With SSI, T1 or T3 gets SERIALIZATION_FAILURE.

### With SSI (Proper Behavior)
```
T1 (SSI): SELECT SUM(balance) FROM accounts;
          -- Creates SIREAD locks on both rows
          COMMIT;  -- Succeeds if no concurrent writes

T2 (SSI): UPDATE accounts SET balance -= 100 WHERE account_id = 1;
          -- Conflicts with T1's read
          UPDATE accounts SET balance += 100 WHERE account_id = 2;
          -- Also conflicts with T1's read
          COMMIT;
          -- Dangerous structure detected: T2 cannot commit
          -- SERIALIZATION_FAILURE

T3 (SSI): BEGIN;
          SELECT SUM(balance) FROM accounts;
          -- Now succeeds, no dangerous structure
          COMMIT;
```

### Resolution Strategy: Order-Dependent Logic
```python
def transfer_money(from_account, to_account, amount):
    """Transfer with deterministic ordering to minimize failures."""
    
    # Enforce ordering: always debit lowest account_id first
    debit_acct = min(from_account, to_account)
    credit_acct = max(from_account, to_account)
    
    if from_account == debit_acct:
        debit_amount = amount
        credit_amount = amount
    else:
        debit_amount = -amount
        credit_amount = -amount
    
    while True:
        try:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            
            # Always update in same order (prevents deadlock + reduces conflicts)
            cursor.execute(
                """UPDATE accounts SET balance = balance + %s 
                   WHERE account_id = %s""",
                (debit_amount, debit_acct)
            )
            cursor.execute(
                """UPDATE accounts SET balance = balance + %s 
                   WHERE account_id = %s""",
                (credit_amount, credit_acct)
            )
            
            conn.commit()
            break
            
        except psycopg2.OperationalError as e:
            if "40001" in str(e):
                conn.rollback()
                time.sleep(random.uniform(0, 0.1))  # Random backoff
                continue
            raise
```

### Key Takeaway
- **Ordered Access**: Always access data in consistent order (lowest ID first)
- **Reduced Conflicts**: Ordering makes conflict patterns more predictable
- **Lower Failure Rate**: Deterministic ordering can reduce serialization failures

---

## Case Study 3: Read-Only Optimization With Deferrable

### Scenario
Long-running analytical query that's read-only, initially marked as DEFERRABLE.

### Query
```sql
BEGIN TRANSACTION 
    ISOLATION LEVEL SERIALIZABLE 
    DEFERRABLE;
    
SELECT COUNT(*), AVG(amount), MAX(amount)
FROM transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days';
-- This query might take 10-30 seconds
```

### Execution Flow
```
1. BEGIN TRANSACTION statement
   └─ GetSerializableTransactionSnapshot()
      ├─ Check: is there concurrent write transaction?
      └─ If NO writes → DEFERRABLE allowed
         ├─ GetSafeSnapshot()
         ├─ Mark as RO_SAFE
         └─ Zero overhead during read

2. During long scan
   └─ PredicateLockRelation(transactions, snapshot)
      ├─ Predicate lock acquired (minimal overhead)
      └─ If concurrent writes appear
         └─ Would cause serialization failure
         └─ But deferrable waits for them...

3. Commit
   └─ PreCommit_CheckForSerializableConflictOut()
      ├─ No outgoing conflicts (read-only)
      ├─ No incoming conflicts (safe snapshot)
      └─ COMMIT succeeds immediately
```

### Performance Characteristics
```
Regular SERIALIZABLE RO:   Lock acquisition cost (minimal)
DEFERRABLE RO with safety: ~0% overhead (no locks needed)
                           + Wait time for conflicting writes to complete

Difference: DEFERRABLE can wait up to server restart for safety.
Cost: Potentially delayed start, guaranteed no failure.
```

### Application Code
```python
def analytical_query():
    """Run long analytical query with deferrable optimization."""
    
    try:
        cursor.execute("""
            BEGIN TRANSACTION 
                ISOLATION LEVEL SERIALIZABLE 
                DEFERRABLE
        """)
        
        # This query is safe from serialization failures
        # If concurrent writes exist during DEFERRABLE phase,
        # the transaction waits until they complete + safe snapshot exists
        
        result = cursor.execute("""
            SELECT COUNT(*), AVG(amount), MAX(amount)
            FROM transactions
            WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
        """).fetchall()
        
        conn.commit()
        return result
        
    except psycopg2.OperationalError as e:
        if "40001" in str(e):
            # With DEFERRABLE, this shouldn't happen
            # But transient network issues could still cause rollback
            conn.rollback()
            raise
```

### When to Use DEFERRABLE
✓ Long-running read-only queries (data warehousing, reporting)
✓ Analytical workloads where accuracy > latency
✓ Queries that can tolerate initial wait for safety

✗ Interactive queries (users expect immediate response)
✗ Queries with side effects (logging, metric updates)
✗ Real-time dashboards (can't wait for safe snapshot)

---

## Case Study 4: Snapshot Conflict With Multiple Readers

### Scenario
Multiple concurrent readers and one writer, dangerous structure emerges.

### Execution Timeline
```
Time  T1 (Reader)              T2 (Reader)             T3 (Writer)
────────────────────────────────────────────────────────────────
 t=0  BEGIN SERIALIZABLE
      → Snapshot: xmin=100
 t=1                           BEGIN SERIALIZABLE
                               → Snapshot: xmin=100
 t=2  SELECT col1 FROM t
      WHERE id IN (1,2,3)
      → Creates SIREAD locks on
        (t, PAGE), (t, TID=1,2,3)
 t=3                                                    BEGIN
                                                        UPDATE t SET col1=X
                                                        WHERE id IN (1,2,3)
                                                        → CheckForSerializableConflictIn()
                                                           finds T1's locks
                                                           → Creates conflict T3→T1
 t=4                           SELECT col2 FROM t
                               WHERE id = 1
                               → Creates SIREAD lock on (t, TID=1)
 t=5                                                    INSERT INTO audit ...
                                                        COMMIT;
                                                        → T3 committed, with outgoing conflict
 t=6  UPDATE t SET col3=Y
      WHERE id = 1
      → CheckForSerializableConflictIn()
         finds T2's locks on (t, TID=1)
         → Creates conflict T1→T2
 t=7  COMMIT
      → PreCommit_CheckForSerializationFailure()
         checks: T3→T1→T2 cycle?
         No direct conflict T2→T3
         ✓ COMMIT succeeds

 t=8                           COMMIT
                               → PreCommit_CheckForSerializationFailure()
                                  Check: T3→T1, T1→T2
                                  → Dangerous structure!
                                  → SERIALIZATION_FAILURE
```

### Why T2 Fails, T1 Succeeds
- T1's writes conflict with T2's read
- But T3's writes happened BEFORE T1's read
- No cycle that makes T2 unsafe

Pattern that would cause T2 failure:
- T3 writes → T1 reads (conflicts)
- T1 writes → T2 reads (conflicts)
- T2 would have conflict_in from T1
- T1 has conflict_out to T3
- → Tin=T3, Tpivot=T1, Tout=T2 (dangerous!)

### Mitigation
```python
def retryable_transaction():
    """Handle read + write transaction with potential failures."""
    
    while True:
        try:
            conn.set_isolation_level(SERIALIZABLE)
            
            # Read phase
            cursor.execute("SELECT col2 FROM t WHERE id = %s", (1,))
            col2_value = cursor.fetchone()
            
            # Write phase (more expensive)
            cursor.execute(
                "UPDATE t SET col3 = %s WHERE id = %s",
                (col2_value, 1)
            )
            
            conn.commit()
            break
            
        except psycopg2.OperationalError as e:
            if "40001" in str(e):
                conn.rollback()
                # Retry: use exponential backoff
                time.sleep(2 ** attempt_count * 0.01)
                continue
```

---

## Common Patterns & Solutions

| Pattern | Risk | Solution |
|---------|------|----------|
| Many readers, few writers | Low conflict rate | Eager retry, short backoff |
| Few readers, many writers | High conflict rate | Batch writers, ordered access |
| Long txns + short txns mixed | Unpredictable | Use DEFERRABLE for long RO |
| Circular update patterns | Very high failure | Enforce access ordering |
| Read-heavy workload | Very low failure | Use standard SSI, monitor |

---

## Prerequisites
- Complete understanding of Architecture Overview
- Understanding of Conflict Graph and Detection concepts
- Familiarity with Dangerous Structure algorithm

## Next Steps
→ [Deep Dives](18_deep_dives.md) for algorithm internals  
→ [Error Modes and Retries](12_error_modes_and_retries.md) for error handling  
→ [Performance and Tuning](11_performance_and_tuning.md) for optimization strategies
