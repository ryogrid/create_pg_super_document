# XLTW_Oper

## Location
src/include/storage/lmgr.h: 35 - 126

## Overview
XLTW_Oper is an enumeration that defines operation types for transaction lock table waiting contexts, used to provide appropriate error messages when waiting for conflicting transactions to complete.

## Definition
```c
typedef enum XLTW_Oper
{
    XLTW_None,
    XLTW_Update,
    XLTW_Delete,
    XLTW_Lock,
    XLTW_LockUpdated,
    XLTW_InsertIndex,
    XLTW_InsertIndexUnique,
    XLTW_FetchUpdated,
    XLTW_RecheckExclusionConstr,
} XLTW_Oper;
```

## Detailed Description
The XLTW_Oper enumeration specifies the type of operation that requires waiting for another transaction to complete. This enum is primarily used with the XactLockTableWait() function to provide contextual error messages when transaction conflicts occur. Each enum value corresponds to a specific database operation that may need to wait for conflicting transactions to commit or abort before proceeding.

When XLTW_None is specified, no error context callback is set up. For all other values, an appropriate error context message is displayed to help users understand what operation was blocked and why.

## Parameters / Member Variables
- `XLTW_None`: No specific operation context; no error callback is set up
- `XLTW_Update`: Waiting while updating a tuple
- `XLTW_Delete`: Waiting while deleting a tuple  
- `XLTW_Lock`: Waiting while locking a tuple
- `XLTW_LockUpdated`: Waiting while locking an updated version of a tuple
- `XLTW_InsertIndex`: Waiting while inserting an index tuple
- `XLTW_InsertIndexUnique`: Waiting while checking uniqueness during index insertion
- `XLTW_FetchUpdated`: Waiting while rechecking an updated tuple
- `XLTW_RecheckExclusionConstr`: Waiting while checking exclusion constraints

## Dependencies
- Functions called/Symbols referenced:
  - Used as parameter type in XactLockTableWait()
  - Used in XactLockTableWaitInfo structure
  - Referenced in MultiXactIdWait() calls

- Called from (representative examples):
  - [XactLockTableWait](XactLockTableWait.md)() in src/backend/storage/lmgr/lmgr.c:658
  - [Do_MultiXactIdWait](../D/Do_MultiXactIdWait.md)() in src/backend/access/heap/heapam.c:7675
  - [MultiXactIdWait](../M/MultiXactIdWait.md)() in src/backend/access/heap/heapam.c:7752
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md)() in src/backend/executor/execIndexing.c:783

## Notes and Other Information
- This enum is part of PostgreSQL's concurrency control mechanism for handling transaction conflicts
- Error messages corresponding to each operation type are defined in XactLockTableWaitErrorCb() function in src/backend/storage/lmgr/lmgr.c
- The enum values provide user-friendly context when operations are blocked waiting for other transactions
- Used extensively throughout heap access methods and index operations to handle multi-version concurrency control (MVCC)
- Critical for providing meaningful error messages during deadlock detection and transaction conflict resolution