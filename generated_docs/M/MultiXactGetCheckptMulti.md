# MultiXactGetCheckptMulti

## Location
[src/backend/access/transam/multixact.c:2274-2295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2274-L2295)

## Overview
Retrieves the current MultiXact data that needs to be saved in a checkpoint record for recovery purposes.

## Definition

```c
void
MultiXactGetCheckptMulti(bool is_shutdown,
						 MultiXactId *nextMulti,
						 MultiXactOffset *nextMultiOffset,
						 MultiXactId *oldestMulti,
						 Oid *oldestMultiDB)
```
## Detailed Description
MultiXactGetCheckptMulti captures the current state of the MultiXact subsystem that must be preserved in checkpoint records. This information is essential for recovery operations, as it allows PostgreSQL to restore the MultiXact state to a consistent point after a crash or restart.

The function operates under MultiXactGenLock protection to ensure atomic access to the shared MultiXact state. It copies the current values of key MultiXact tracking variables into the provided output parameters, which are then written to the checkpoint record.

The captured information includes the next MultiXact ID to be assigned, the next offset for member storage, and information about the oldest MultiXact still in use, which is crucial for vacuum and wraparound prevention.

## Parameters / Member Variables
- : Boolean indicating whether this is called during database shutdown (currently unused in the function)
- : Output parameter to receive the next MultiXact ID to be assigned
- : Output parameter to receive the next offset for storing MultiXact members
- : Output parameter to receive the oldest MultiXact ID still in use
- : Output parameter to receive the database OID containing the oldest MultiXact

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - debug_elog6
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md)

## Notes and Other Information
- Must be called with appropriate output parameters allocated by the caller
- Uses shared lock (LW_SHARED) on MultiXactGenLock for thread-safe access to MultiXactState
- Includes debug logging to track checkpoint MultiXact values for troubleshooting
- The is_shutdown parameter is accepted but not currently used in the implementation
- Critical for crash recovery and ensuring MultiXact consistency across database restarts
- The captured data becomes part of the checkpoint record written to WAL