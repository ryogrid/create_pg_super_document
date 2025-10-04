# vacuum_xid_failsafe_check

## Location
[src/backend/commands/vacuum.c:1251-1312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L1251-L1312)

## Overview
Determines if a table's relfrozenxid and relminmxid are dangerously far in the past, triggering VACUUM's wraparound failsafe mechanism when necessary.

## Definition
```c
bool
vacuum_xid_failsafe_check(const struct VacuumCutoffs *cutoffs)
```

## Detailed Description
The vacuum_xid_failsafe_check function implements PostgreSQL's failsafe mechanism to prevent transaction ID wraparound disasters. It examines a table's frozen transaction ID (relfrozenxid) and minimum multixact ID (relminmxid) to determine if they have aged beyond critical thresholds.

When either the regular transaction ID or multixact ID becomes too old relative to the current system state, the function returns true to trigger failsafe mode. In failsafe mode, VACUUM becomes more aggressive and may skip certain optimizations (like index vacuuming) to prioritize advancing the frozen XIDs and preventing wraparound.

The function uses configurable thresholds (vacuum_failsafe_age and vacuum_multixact_failsafe_age) but ensures they are at least 105% of the respective freeze_max_age values to provide a safety margin.

## Parameters / Member Variables
- `cutoffs`: VacuumCutoffs structure containing the table's current relfrozenxid and relminmxid values

## Dependencies
- Functions called/Symbols referenced:
  - [ReadNextTransactionId](../R/ReadNextTransactionId.md)
  - [ReadNextMultiXactId](../R/ReadNextMultiXactId.md)
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
- Called from (representative examples):
  - [lazy_check_wraparound_failsafe](../l/lazy_check_wraparound_failsafe.md) (src/backend/access/heap/vacuumlazy.c:2306)

## Notes and Other Information
- Returns true when failsafe should be triggered, false otherwise
- Uses both regular transaction ID and multixact ID age checking
- Provides critical wraparound protection as a last resort mechanism
- Thresholds are configurable but have minimum safety margins (105% of freeze_max_age)
- Part of PostgreSQL's multi-layered defense against transaction ID wraparound
- When triggered, causes VACUUM to prioritize advancing frozen XIDs over other optimizations
- Location: src/backend/commands/vacuum.c:1251-1312

## Simplified Source

```c
bool
vacuum_xid_failsafe_check(const struct VacuumCutoffs *cutoffs)
{
    TransactionId relfrozenxid = cutoffs->relfrozenxid;
    MultiXactId relminmxid = cutoffs->relminmxid;
    TransactionId xid_skip_limit;
    MultiXactId multi_skip_limit;
    int skip_index_vacuum;

    // Check regular transaction ID age
    skip_index_vacuum = Max(vacuum_failsafe_age, autovacuum_freeze_max_age * 1.05);
    xid_skip_limit = ReadNextTransactionId() - skip_index_vacuum;
    if (!TransactionIdIsNormal(xid_skip_limit))
        xid_skip_limit = FirstNormalTransactionId;

    if (TransactionIdPrecedes(relfrozenxid, xid_skip_limit)) {
        // Table's relfrozenxid is too old
        return true;
    }

    // Check multixact ID age
    skip_index_vacuum = Max(vacuum_multixact_failsafe_age,
                           autovacuum_multixact_freeze_max_age * 1.05);
    multi_skip_limit = ReadNextMultiXactId() - skip_index_vacuum;
    if (multi_skip_limit < FirstMultiXactId)
        multi_skip_limit = FirstMultiXactId;

    if (MultiXactIdPrecedes(relminmxid, multi_skip_limit)) {
        // Table's relminmxid is too old
        return true;
    }

    return false;
}
```