# XactLockTableWaitErrorCb

## Location
[src/backend/storage/lmgr/lmgr.c:838-902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L838-L902)

## Overview
Error context callback function that provides detailed context information when transaction lock waits encounter errors or timeouts.

## Definition
```c
static void XactLockTableWaitErrorCb(void *arg)
```

## Detailed Description
This function serves as an error context callback that is registered during transaction lock wait operations. When an error occurs (such as a deadlock or timeout) while waiting for a transaction lock, this callback provides meaningful context about what operation was being performed when the wait occurred.

The function examines the XactLockTableWaitInfo structure passed as an argument and generates appropriate error context messages based on the type of operation that was in progress. It handles various types of operations including updates, deletes, locks, index insertions, uniqueness checks, and exclusion constraint checks.

The callback formats human-readable error messages that include the specific tuple being operated on (block number and offset) and the relation name, helping users and administrators understand exactly which data was involved in the lock conflict.

## Parameters / Member Variables
- `arg`: Pointer to XactLockTableWaitInfo structure containing context information about the lock wait operation

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - RelationIsValid
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - RelationGetRelationName
  - errcontext
  - gettext_noop (for internationalization)
- Called from (representative examples):
  - [XactLockTableWait](XactLockTableWait.md) (in lmgr.c:678)

## Notes and Other Information
- This is a static function, only used within the lmgr.c module
- Supports multiple operation types through the XLTW_* enum values:
  - XLTW_Update: Tuple update operations
  - XLTW_Delete: Tuple deletion operations  
  - XLTW_Lock: Tuple locking operations
  - XLTW_LockUpdated: Locking updated tuple versions
  - XLTW_InsertIndex: Index tuple insertion
  - XLTW_InsertIndexUnique: Uniqueness checking during insertion
  - XLTW_FetchUpdated: Rechecking updated tuples
  - XLTW_RecheckExclusionConstr: Exclusion constraint validation
- Uses gettext_noop for internationalization support of error messages
- Provides tuple-level granularity in error reporting with block and offset numbers
- Essential for debugging lock contention and deadlock scenarios in PostgreSQL