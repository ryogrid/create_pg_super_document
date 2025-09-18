# GetRunningTransactionLocks

## Location
[src/backend/storage/lmgr/lock.c:3988-4069](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L3988-L4069)

## Overview
GetRunningTransactionLocks returns a list of currently held AccessExclusiveLocks on relations for use by LogStandbySnapshot in WAL-based replication scenarios.

## Definition


## Detailed Description
This function scans the lock manager's shared lock table to identify all currently granted AccessExclusiveLocks on relations. It's specifically designed for use by the standby snapshot logging mechanism in PostgreSQL's streaming replication system.

The function operates by:
1. **Lock table acquisition**: Takes shared locks on all lock table partitions to ensure consistent data
2. **Space allocation**: Allocates memory for the worst-case scenario (all locks being AccessExclusiveLocks)
3. **Lock filtering**: Scans all PROCLOCKs and identifies those that hold AccessExclusiveLocks on relations
4. **Transaction validation**: Filters out locks from transactions that have already committed but not yet released locks
5. **Data collection**: Extracts transaction ID, database OID, and relation OID for each qualifying lock

The function is optimized for the specific case of AccessExclusiveLocks, which can only have one holder, avoiding the complexity of duplicate lock handling that would be required for shared lock types.

## Parameters / Member Variables
- : Output parameter that receives the number of AccessExclusiveLocks found

**Return value**:  - An array of xl_standby_lock structures, each containing:
- : Transaction ID of the lock holder
- : Database OID where the locked relation exists
- : Relation OID of the locked object

## Dependencies
- Functions called/Symbols referenced:
  - ,  - Lock table partition management
  -  - Count total PROCLOCKs in the hash table
  - ,  - Hash table iteration
  -  - Memory allocation for result array
  -  - Access individual lock partitions
  -  - Lock mode bit manipulation
  -  - Transaction ID validation

- Called from (representative examples):
  -  - WAL logging for standby server consistency

## Notes and Other Information
- Only considers AccessExclusiveLocks on relations (LOCKTAG_RELATION), ignoring other lock types and targets
- Takes a conservative approach by locking all partitions simultaneously, though the comment suggests optimization possibilities using reference counting
- Filters out locks from transactions that have committed but not yet released locks by checking transaction ID validity
- Uses the same lock acquisition ordering as other lock table scanning functions to avoid deadlocks
- Memory allocation is deliberately oversized for simplicity and performance, allocating space for all possible locks
- The function guarantees that AccessExclusiveLocks are never duplicated in the result since they can only have one holder
- Critical for maintaining consistency in streaming replication by ensuring standby servers are aware of exclusive locks that could affect recovery