# BecomeLockGroupLeader

## Location
[src/backend/storage/lmgr/proc.c:1898-1927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L1898-L1927)

## Overview
Designates the current process as a lock group leader, enabling other processes to join the group for coordinated lock management.

## Definition
void BecomeLockGroupLeader(void)

## Detailed Description
BecomeLockGroupLeader transforms the current process into a lock group leader, establishing a group structure for coordinated lock management among multiple processes. The function first checks if the process is already a leader to avoid redundant operations, then asserts that the process is not currently a follower of another group. It creates a single-member lock group by setting the process as its own leader and adding itself to the group's member list. The operation is protected by acquiring the appropriate hash partition lock to ensure atomicity during the group setup.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - LockHashPartitionLockByProc
  - LWLockAcquire
  - LWLockRelease
  - [dlist_push_head](../d/dlist_push_head.md)
  - [LWLock](../L/LWLock.md)
- Called from (representative examples):
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md)

## Notes and Other Information
- Idempotent operation - can be safely called multiple times on the same process
- Process must not already be a member of another lock group (asserted)
- Uses hash partitioning to determine the appropriate lock for the operation
- Initializes the group with the leader as the only member
- Essential for parallel query processing where workers need coordinated lock management
- The leader process coordinates lock acquisition/release for all group members
- After becoming leader, other processes can join using BecomeLockGroupMember