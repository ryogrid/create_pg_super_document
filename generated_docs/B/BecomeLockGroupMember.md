# BecomeLockGroupMember

## Location
[src/backend/storage/lmgr/proc.c:1928-1962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L1928-L1962)

## Overview
Designates the current process as a member of an existing lock group led by the specified leader process, with validation to handle potential leader process recycling.

## Definition
bool BecomeLockGroupMember(PGPROC *leader, int pid)

## Detailed Description
BecomeLockGroupMember allows a process to join an existing lock group under a designated leader process. The function includes robust handling for the scenario where the leader process might exit and its PGPROC structure could be recycled before the join operation completes. To prevent joining a recycled PGPROC belonging to an unrelated process, the function requires both the leader PGPROC pointer and the leader's PID as an interlock mechanism. The function verifies that the leader is still active and is indeed a group leader before adding the current process to the group's member list. The operation is protected by acquiring the appropriate hash partition lock.

## Parameters / Member Variables
- : Pointer to the PGPROC structure of the intended group leader
- : Process ID of the intended leader used as validation interlock

## Dependencies
- Functions called/Symbols referenced:
  - LockHashPartitionLockByProc
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - [PGPROC](../P/PGPROC.md)
  - [LWLock](../L/LWLock.md)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)

## Notes and Other Information
- Returns true if successfully joined the group, false otherwise
- Process must not already be a group leader or member (asserted)
- PID parameter prevents joining a recycled PGPROC of an unrelated process
- Uses hash partitioning to determine the correct lock regardless of PGPROC recycling
- Validates that the target process is still the intended leader before joining
- Essential for parallel workers to coordinate lock management with their leader
- The PID interlock ensures safety even if the leader PGPROC gets recycled
- Adds the process to the tail of the leader's member list upon successful join