# VirtualTransactionId

## Location
[src/include/storage/lock.h:63-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L63-L64)

## Overview
VirtualTransactionId is a structure that uniquely identifies a virtual transaction within a PostgreSQL backend process. It combines a process number with a local transaction ID to create a system-wide unique identifier for transactions.

## Definition

```c
typedef struct
{
	ProcNumber	procNumber;		/* proc number of the PGPROC */
	LocalTransactionId localTransactionId;	/* lxid from PGPROC */
} VirtualTransactionId;
```
## Detailed Description
The VirtualTransactionId structure provides a lightweight way to identify transactions across the PostgreSQL system. Unlike regular transaction IDs (XIDs), virtual transaction IDs are assigned locally by each backend process and do not require global coordination. This makes them suitable for identifying transactions that may not need to persist (such as read-only transactions) or for internal tracking purposes.

The structure is deliberately kept separate from the PGPROC structure to prevent accidental struct assignment operations. Instead, developers are encouraged to use the GET_VXID_FROM_PGPROC() macro to safely extract virtual transaction IDs.

Virtual transaction IDs are particularly important for:
- Lock management and conflict resolution
- Recovery and standby operations
- Transaction snapshot management
- Checkpoint coordination

## Parameters / Member Variables
- `procNumber`: The process number of the PGPROC structure that owns this virtual transaction. This identifies which backend process is running the transaction.
- `localTransactionId`: The local transaction ID assigned by the owning process. This is a process-local counter that makes each virtual transaction unique within that process.
## Dependencies
- Functions called/Symbols referenced: None directly
- Called from (representative examples):
  - [GetCurrentVirtualXIDs](../G/GetCurrentVirtualXIDs.md)
  - [GetConflictingVirtualXIDs](../G/GetConflictingVirtualXIDs.md)
  - [VirtualXactLock](VirtualXactLock.md)
  - [ResolveRecoveryConflictWithVirtualXIDs](../R/ResolveRecoveryConflictWithVirtualXIDs.md)
  - [GetLockConflicts](../G/GetLockConflicts.md)

## Notes and Other Information
- The structure includes several utility macros for validation and comparison:
  - : Checks if a virtual transaction ID is valid
  - : Compares two virtual transaction IDs
  - : Checks if the virtual transaction ID represents a recovered prepared transaction
- Virtual transaction IDs with  set to  represent recovered prepared transactions
- The design emphasizes safety by discouraging direct struct assignment in favor of dedicated macros
- Virtual transaction IDs are essential for PostgreSQL's multi-version concurrency control (MVCC) system and recovery mechanisms