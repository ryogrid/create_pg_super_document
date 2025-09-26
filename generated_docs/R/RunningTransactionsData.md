# RunningTransactionsData

## Location
src/include/storage/standby.h: 86 - 98

## Overview
RunningTransactionsData is a structure that encapsulates information about currently running transactions, used primarily for hot standby and WAL replay operations to maintain transaction visibility and consistency.

## Definition

```c
typedef struct RunningTransactionsData
{
	int			xcnt;			/* # of xact ids in xids[] */
	int			subxcnt;		/* # of subxact ids in xids[] */
	subxids_array_status subxid_status;
	TransactionId nextXid;		/* xid from TransamVariables->nextXid */
	TransactionId oldestRunningXid; /* *not* oldestXmin */
	TransactionId oldestDatabaseRunningXid; /* same as above, but within the
											 * current database */
	TransactionId latestCompletedXid;	/* so we can set xmax */

	TransactionId *xids;		/* array of (sub)xids still running */
} RunningTransactionsData;
```
## Detailed Description
This structure serves as a snapshot of the current transaction state, containing essential information for maintaining MVCC (Multi-Version Concurrency Control) consistency in PostgreSQL. It is particularly critical for hot standby operations where the standby server needs to understand which transactions are currently active on the primary server to properly handle query visibility and avoid conflicts.

The structure captures both main transactions and subtransactions, along with metadata about transaction ID boundaries and the status of subtransaction information completeness.

## Parameters / Member Variables
- : Number of main transaction IDs stored in the xids array
- : Number of subtransaction IDs stored in the xids array
- : Indicates the completeness and location of subtransaction information (enum subxids_array_status)
- : The next transaction ID that will be assigned, copied from TransamVariables->nextXid
- : The oldest transaction ID that is still running (not the same as oldestXmin)
- : Similar to oldestRunningXid but scoped to the current database only
- : The most recent transaction ID that has completed, used for setting xmax values
- : Dynamic array containing the actual transaction IDs (both main transactions and subtransactions) that are currently running

## Dependencies
- Functions called/Symbols referenced:
  - subxids_array_status
  - TransactionId
- Called from (representative examples):
  - StartupXLOG
  - xlog_redo
  - GetRunningTransactionData
  - standby_redo
  - RunningTransactions

## Notes and Other Information
- This structure is central to PostgreSQL's hot standby functionality, enabling read-only queries on standby servers
- The xids array is dynamically allocated and contains a mix of main transaction IDs and subtransaction IDs
- The subxid_status field is crucial for understanding whether all subtransaction information is available or if some has been lost due to overflow conditions
- Used extensively in WAL (Write-Ahead Log) replay operations to maintain proper transaction visibility semantics