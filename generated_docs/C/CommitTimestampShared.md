# CommitTimestampShared

## Location
src/backend/access/transam/commit_ts.c: 98 - 103

## Overview
CommitTimestampShared is a shared memory structure that caches the last committed transaction's timestamp information and tracks the activation status of the commit timestamp feature.

## Definition


## Detailed Description
CommitTimestampShared is a critical shared memory structure in PostgreSQL's commit timestamp tracking system. It serves as a performance optimization by caching the most recently committed transaction's information, avoiding the need to access the SLRU buffers for the last commit data in many cases.

The structure also maintains the activation status of the commit timestamp feature, which is particularly important in replication scenarios. The activation status is kept separate from the GUC (Grand Unified Configuration) parameter to allow standby servers to activate the module independently if the primary server has it active, ensuring consistency across the replication cluster.

Access to this structure is protected by CommitTsLock, though in some specific cases, the commitTsActive field may be read without acquiring the lock when performance considerations outweigh strict consistency requirements (such cases are documented with rationale comments in the code).

## Parameters / Member Variables
- : A TransactionId representing the transaction ID of the most recently committed transaction
- : A CommitTimestampEntry containing the complete timestamp and origin information for the most recently committed transaction
- : A boolean flag indicating whether commit timestamp tracking is currently active in the system

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (type)
  - [CommitTimestampEntry](CommitTimestampEntry.md) (struct)
- Called from (representative examples):
  - CommitTsShmemSize (function - for memory allocation sizing)
  - CommitTsShmemInit (function - for shared memory initialization)

## Notes and Other Information
- Protected by CommitTsLock for thread-safe access across multiple backend processes
- The cached data helps avoid expensive SLRU buffer lookups for the most common case of querying the last committed transaction
- The commitTsActive flag allows standby servers to independently track activation status from the primary server
- This structure is allocated in PostgreSQL's shared memory segment during server startup
- Reading commitTsActive without the lock is permitted in specific performance-critical paths where the race condition is acceptable
- The separation of activation status from GUC settings enables proper replication behavior where standby nodes can activate commit timestamp tracking based on primary server state rather than local configuration