# GetNextLocalTransactionId

## Location
[src/backend/storage/ipc/sinvaladt.c:743-754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/sinvaladt.c#L743-L754)

## Overview
Allocates a new LocalTransactionId for building VirtualTransactionIds without requiring shared memory contention during normal operation.

## Definition
```c
LocalTransactionId GetNextLocalTransactionId(void)
```

## Detailed Description
GetNextLocalTransactionId is a lightweight function that provides the low-order part of PostgreSQL's VirtualTransactionId system. VirtualTransactionIds are split into two components: a high-order ProcNumber (identifying the backend) and a low-order LocalTransactionId (uniquely identifying transactions within that backend).

This design allows new transaction IDs to be allocated without shared memory contention, since each backend maintains its own local counter (nextLocalTransactionId). The function implements a simple increment-and-validate loop to ensure that invalid transaction IDs are skipped during wraparound conditions.

The sequential allocation of local transaction IDs across successive processes using the same PGPROC slot helps prevent VirtualTransactionId reuse within short time intervals, maintaining transaction uniqueness guarantees across backend restarts.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - LocalTransactionIdIsValid (validates the generated transaction ID)
  - LocalTransactionId (transaction ID type)
  - nextLocalTransactionId (backend-local counter variable)
- Called from (representative examples):
  - [StartTransaction](../S/StartTransaction.md) (xact.c:2112) - [when](../w/when.md) beginning new transactions
  - [InitRecoveryTransactionEnvironment](../I/InitRecoveryTransactionEnvironment.md) (standby.c:140) - during recovery setup

## Notes and Other Information
- Uses a backend-local counter (nextLocalTransactionId) to avoid shared memory contention
- Implements wraparound protection by skipping invalid LocalTransactionId values
- Part of PostgreSQL's VirtualTransactionId system combining ProcNumber + LocalTransactionId
- Enables lock-free transaction ID allocation during normal operation
- Successive backends in the same PGPROC slot use consecutive ID sequences to prevent reuse
- The nextLocalTransactionId counter is copied from the previous backend's value during initialization
- Critical for transaction management and virtual transaction identification throughout PostgreSQL
- Simple but essential component of the transaction system's scalability design