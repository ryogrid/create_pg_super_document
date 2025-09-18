# ProcArrayStruct

## Location
[src/backend/storage/ipc/procarray.c:71-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L71-L100)

## Overview
ProcArrayStruct is the core shared memory structure that maintains global transaction state information, tracking active processes and transaction IDs for concurrency control and snapshot management in PostgreSQL.

## Definition


## Detailed Description
ProcArrayStruct serves as the central repository for tracking all active backend processes and their transaction states in PostgreSQL's shared memory. This structure is critical for implementing MVCC (Multi-Version Concurrency Control) by maintaining information about which transactions are currently running, which XIDs have been assigned, and what the global transaction visibility horizons are.

The structure manages two primary areas of concern: active process tracking through the pgprocnos array, and known assigned transaction IDs through a circular buffer mechanism. It also maintains replication slot information to prevent premature cleanup of data that might still be needed by logical replication.

## Parameters / Member Variables
- : Current number of active backend processes registered in the procarray
- : Maximum number of processes that can be accommodated (allocated array size)
- : Allocated size of the known assigned XIDs circular buffer
- : Current number of valid entries in the known assigned XIDs array
- : Array index pointing to the oldest valid known assigned XID entry
- : Array index pointing to the position after the newest entry (insertion point)
- : Highest subtransaction ID that was removed due to array overflow, used to track potential missing subxids
- : Oldest transaction ID that any replication slot still needs for data visibility
- : Oldest catalog transaction ID needed by any replication slot for DDL changes
- : Flexible array member containing indexes into the global allProcs array, referencing active PGPROC entries

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - TransactionId
  - PROCARRAY_MAXPROCS

- Called from (representative examples):
  - [CreateSharedProcArray](../C/CreateSharedProcArray.md)
  - [ProcArrayAdd](ProcArrayAdd.md)
  - [ProcArrayRemove](ProcArrayRemove.md)
  - ComputeXidHorizons
  - [GetSnapshotData](../G/GetSnapshotData.md)
  - TransactionIdIsInProgress
  - [GetOldestActiveTransactionId](../G/GetOldestActiveTransactionId.md)

## Notes and Other Information
- This structure resides in shared memory and is protected by ProcArrayLock
- The known assigned XIDs mechanism is primarily used during recovery to track transaction IDs that have been assigned but may not yet be visible in PGPROC entries
- The circular buffer design for known assigned XIDs allows efficient insertion and removal while preventing unbounded growth
- Replication slot tracking prevents vacuum from removing data that downstream replicas might still need
- Access to this structure requires appropriate locking: shared lock for reads, exclusive lock for modifications
- The flexible array member pgprocnos allows the structure size to be determined at startup based on max_connections configuration