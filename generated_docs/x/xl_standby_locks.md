# xl_standby_locks

## Location
[src/include/storage/standbydefs.h:38-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/standbydefs.h#L38-L42)

## Overview
A WAL record structure that contains an array of AccessExclusiveLock information for standby recovery, used to maintain lock state consistency during hot standby operations.

## Definition

```c
typedef struct xl_standby_locks
{
	int			nlocks;			/* number of entries in locks array */
	xl_standby_lock locks[FLEXIBLE_ARRAY_MEMBER];
} xl_standby_locks;
```
## Detailed Description
The  structure is a WAL record format used in PostgreSQL's standby recovery system to log and replay AccessExclusiveLocks. This structure is part of the standby resource manager (RM_STANDBY_ID) and is written to the WAL when AccessExclusiveLocks need to be communicated to standby servers during hot standby mode.

When a transaction acquires AccessExclusiveLocks on tables, these locks must be replicated on standby servers to prevent read-only queries on the standby from conflicting with operations on the primary. The  record packages multiple lock entries into a single WAL record for efficient logging and replay.

During WAL replay on standby servers, this record is processed by the  function, which iterates through the locks array and calls  for each lock entry to establish the same lock state on the standby.

## Parameters / Member Variables
- : The number of lock entries contained in the locks array
- : A flexible array member containing  structures, each representing an individual AccessExclusiveLock

## Dependencies
- Functions called/Symbols referenced:
  - xl_standby_lock
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - LogAccessExclusiveLocks
  - standby_redo
  - standby_desc

## Notes and Other Information
- This structure uses a flexible array member to accommodate variable numbers of locks in a single WAL record
- Only AccessExclusiveLocks are logged for standby recovery; other lock types do not need to be replicated
- Records of this type are only generated when  returns true
- The WAL record is marked as unimportant () during insertion
- Processing on standby servers is conditional on standby state not being 