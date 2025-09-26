# GetLockStatusData

## Location
[src/backend/storage/lmgr/lock.c:3621-3812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L3621-L3812)

## Overview
GetLockStatusData returns a summary of the lock manager's internal status for use in user-level reporting functions, providing a snapshot of all currently held and waiting locks in the system.

## Definition

```c
structure.  We do
	 * this so that, at least for locks in the primary lock table, the state
	 * will be self-consistent.
	 *
	 * Since this is a read-only operation, we take shared instead of
	 * exclusive lock.  There's not a whole lot of point to this, because all
	 * the normal operations require exclusive lock, but it doesn't hurt
	 * anything either. It will at least allow two backends to do
	 * GetLockStatusData in parallel.
	 *
	 * Must grab LWLocks in partition-number order to avoid LWLock deadlock.
	 */
	for (i = 0;
```
## Detailed Description
This function creates a comprehensive snapshot of the PostgreSQL lock manager's state by collecting information from both the fast-path lock arrays and the main lock hash table. It's designed to minimize the time spent holding LWLocks by quickly copying necessary data and releasing locks, allowing callers to process the data without blocking normal lock operations.

The function operates in two phases:
1. **Fast-path lock collection**: Iterates through each backend's fast-path lock arrays, collecting relation locks and virtual transaction locks that don't require entries in the main lock table
2. **Main lock table scan**: Acquires shared locks on all lock partitions and scans the PROCLOCK hash table to collect information about locks that couldn't use the fast-path mechanism

The returned data consists of LockInstanceData objects, which are abstracted versions of PROCLOCK data structures. Each entry represents a unique combination of a lock and an interested process.

## Parameters / Member Variables
- **Return value**:  - A dynamically allocated structure containing:
  - : Array of LockInstanceData objects describing each lock instance
  - : Total number of lock instances in the array

## Dependencies
- Functions called/Symbols referenced:
  - ,  - Memory allocation functions
  - ,  - Lock acquisition and release
  - , ,  - [Hash](../H/Hash.md) table operations
  -  - Fast-path lock bit manipulation
  - ,  - Lock tag construction
  -  - Lock partition access
  -  - Atomic read operations

- Called from (representative examples):
  -  - SQL function for displaying lock status

## Notes and Other Information
- The function prioritizes consistency over perfect accuracy, as taking all locks simultaneously would be impractical
- Fast-path locks are processed first with individual backend locks, which may create slight timing inconsistencies
- Main lock table processing uses shared locks on all partitions to ensure consistency within the primary lock structures
- Lock partitions are acquired in order and released in reverse order to avoid deadlocks and optimize performance
- The function handles dynamic memory allocation, expanding the result array as needed during collection
- Virtual transaction locks in fast-path arrays are included when  is set
- Wait start times are captured atomically to provide accurate timing information for lock waits