# DoLockModesConflict

## Location
[src/backend/storage/lmgr/lock.c:570-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L570-L589)

## Overview
A utility function that determines whether two lock modes would conflict with each other by consulting the default lock method's conflict table.

## Definition
bool DoLockModesConflict(LOCKMODE mode1, LOCKMODE mode2)

## Detailed Description
DoLockModesConflict provides a simple interface to check lock mode compatibility using PostgreSQL's default lock method. The function accesses the conflict table from the default lock method and uses bitwise operations to determine if the two specified lock modes would conflict.

The conflict determination is based on a pre-computed conflict matrix stored in the lock method table, where each lock mode has an associated bitmask indicating which other modes it conflicts with. The function converts mode2 to a bit position using LOCKBIT_ON() and checks if that bit is set in mode1's conflict mask.

## Parameters / Member Variables
- mode1: The first lock mode to check for conflicts
- mode2: The second lock mode to check against mode1

## Dependencies
- Functions called/Symbols referenced:
  - LockMethods: Global array of lock method tables
  - DEFAULT_LOCKMETHOD: Index for the default lock method
  - LOCKBIT_ON: Macro to convert lock mode to bit position for conflict checking
- Called from (representative examples):
  - [test_lockmode_for_conflict](../t/test_lockmode_for_conflict.md): Used in heap access method for conflict testing
  - [DoesMultiXactIdConflict](DoesMultiXactIdConflict.md): Used in multitransaction conflict detection
  - [Do_MultiXactIdWait](Do_MultiXactIdWait.md): Used during multitransaction waiting operations
  - [initialize_reloptions](../i/initialize_reloptions.md): Used during relation options initialization
  - LockHashPartitionLockByProc: Used in lock hash partition operations

## Notes and Other Information
- Uses the DEFAULT_LOCKMETHOD for conflict resolution, which is the standard table-level locking method
- The conflict table is bidirectional - if mode1 conflicts with mode2, then mode2 also conflicts with mode1
- This function provides a clean abstraction over the low-level conflict table access
- Essential for deadlock detection and lock compatibility checking throughout PostgreSQL
- The conflict table is initialized during system startup with predefined lock mode relationships

## Simplified Source

```c
bool DoLockModesConflict(LOCKMODE mode1, LOCKMODE mode2)
{
    LockMethod lockMethodTable = LockMethods[DEFAULT_LOCKMETHOD];

    if (lockMethodTable->conflictTab[mode1] & LOCKBIT_ON(mode2))
        return true;

    return false;
}
```

This function checks if two lock modes conflict by:
1. Getting the default lock method table
2. Checking if mode1's conflict mask has the bit for mode2 set
3. Returning true if they conflict, false otherwise