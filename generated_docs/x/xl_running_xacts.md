# xl_running_xacts

## Location
[src/include/storage/standbydefs.h:47-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/standbydefs.h#L47-L57)

## Overview
A WAL record structure that captures a snapshot of currently running transactions for standby recovery, containing transaction state information needed to maintain proper snapshot isolation on standby servers.

## Definition

```c
typedef struct xl_running_xacts
{
	int			xcnt;			/* # of xact ids in xids[] */
	int			subxcnt;		/* # of subxact ids in xids[] */
	bool		subxid_overflow;	/* snapshot overflowed, subxids missing */
	TransactionId nextXid;		/* xid from TransamVariables->nextXid */
	TransactionId oldestRunningXid; /* *not* oldestXmin */
	TransactionId latestCompletedXid;	/* so we can set xmax */

	TransactionId xids[FLEXIBLE_ARRAY_MEMBER];
} xl_running_xacts;
```
## Detailed Description
The  structure is a WAL record format used in PostgreSQL's standby recovery system to maintain consistent snapshots of running transactions. This structure captures the essential transaction state information from the primary server and transmits it to standby servers, allowing them to maintain proper snapshot isolation for read-only queries.

This record is periodically logged by  and contains both main transaction IDs and subtransaction IDs in the flexible array member. The structure mirrors  but is designed as a contiguous memory block suitable for WAL storage.

During WAL replay, standby servers use this information via  to update their local transaction state, ensuring that snapshot visibility rules are correctly applied to read-only queries. The structure is also used by logical decoding in  for maintaining consistent snapshots during logical replication.

## Parameters / Member Variables
- : Number of main transaction IDs in the xids array
- : Number of subtransaction IDs in the xids array  
- : Boolean flag indicating whether the snapshot overflowed and some subtransaction IDs are missing
- : The next transaction ID to be assigned (from TransamVariables->nextXid)
- : The oldest currently running transaction ID (not the same as oldestXmin)
- : The most recently completed transaction ID, used to set xmax for snapshots
- : Flexible array containing the actual transaction IDs (both main transactions and subtransactions)

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - TransactionId
- Called from (representative examples):
  - [LogCurrentRunningXacts](../L/LogCurrentRunningXacts.md)
  - [standby_redo](../s/standby_redo.md)
  - [SnapBuildProcessRunningXacts](../S/SnapBuildProcessRunningXacts.md)
  - [standby_decode](../s/standby_decode.md)
  - [standby_desc](../s/standby_desc.md)

## Notes and Other Information
- Records are marked with  as they are not critical for durability
- The structure uses  macro to calculate the minimum size without the flexible array
- Main transactions and subtransactions are stored together in the xids array, with xcnt + subxcnt total entries
- When  is true, it indicates that not all subtransactions could be captured in the snapshot
- The record is used both for hot standby recovery and logical decoding snapshot building
- Logging is asynchronous - records are nudged to disk via  rather than forced immediately