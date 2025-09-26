# GetNewTransactionId

## Location
[src/backend/access/transam/varsup.c:77-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/varsup.c#L77-L287)

## Overview
GetNewTransactionId allocates the next FullTransactionId for a new transaction or subtransaction while implementing critical safety checks to prevent XID wraparound.

## Definition
```c
FullTransactionId GetNewTransactionId(bool isSubXact)
```

## Detailed Description
GetNewTransactionId is the core function responsible for allocating new transaction identifiers in PostgreSQL. It manages the global transaction counter in shared memory and implements critical safety mechanisms to prevent catastrophic XID wraparound. The function handles both regular transactions and subtransactions, enforcing limits through vacuum triggers, warnings, and ultimately preventing new transaction assignment when approaching wraparound thresholds. It also manages the shared ProcArray state and extends various transaction-related logs (CLOG, SUBTRANS, CommitTs) as needed.

The function implements a multi-layered protection system:
- Issues autovacuum requests when approaching xidVacLimit
- Issues warnings when approaching xidWarnLimit  
- Completely blocks new transactions when approaching xidStopLimit
- Special handling for bootstrap mode and recovery scenarios

## Parameters / Member Variables
- `isSubXact`: Boolean indicating whether this XID is for a subtransaction (true) or main transaction (false)

## Dependencies
- Functions called/Symbols referenced:
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - IsBootstrapProcessingMode
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - XidFromFullTransactionId
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md)
  - [ExtendCLOG](../E/ExtendCLOG.md)
  - [ExtendCommitTs](../E/ExtendCommitTs.md)
  - [ExtendSUBTRANS](../E/ExtendSUBTRANS.md)
  - [FullTransactionIdAdvance](../F/FullTransactionIdAdvance.md)
  - [SendPostmasterSignal](../S/SendPostmasterSignal.md)
  - [get_database_name](../g/get_database_name.md)
- Called from (representative examples):
  - [AssignTransactionId](../A/AssignTransactionId.md)

## Notes and Other Information
- Located in src/backend/access/transam/varsup.c:77-287
- Acquires XidGenLock (LW_EXCLUSIVE) for safe concurrent access
- Implements XID wraparound protection through multiple threshold checks
- Updates both MyProc->xid and ProcGlobal->xids[] for visibility
- Handles subtransaction overflow by setting cache-overflowed flag
- Uses write barriers to prevent dangerous code reordering
- Cannot be called during parallel operations or recovery
- Returns special BootstrapTransactionId during bootstrap processing
- Critical for maintaining ACID properties and preventing data loss from XID wraparound