# PGOutputTxnData

## Location
[src/backend/replication/pgoutput/pgoutput.c:211-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L211-L214)

## Overview
PGOutputTxnData is a struct used in PostgreSQL's pgoutput logical replication plugin to track transaction-level state, specifically whether a BEGIN message has been sent for a transaction to optimize network bandwidth by avoiding empty transaction messages.

## Definition

```c
typedef struct PGOutputTxnData
{
	bool		sent_begin_txn; /* flag indicating whether BEGIN has been sent */
} PGOutputTxnData;
```
## Detailed Description
PGOutputTxnData implements a bandwidth optimization in logical replication by tracking whether a transaction has actually sent any changes to the subscriber. The structure maintains per-transaction state to determine if a BEGIN message needs to be sent. BEGIN is only sent when the first actual change in a transaction is processed, allowing the system to skip sending BEGIN/COMMIT message pairs for empty transactions.

This optimization is specifically disabled for prepared transactions to avoid protocol inconsistencies that could occur if the WALSender restarts between PREPARE and COMMIT PREPARED phases. It is also disabled for streamed transactions since they can contain prepared transactions. The design prioritizes correctness over optimization in complex transaction scenarios.

## Parameters / Member Variables
- : Boolean flag indicating whether a BEGIN message has been sent for the current transaction. Initially false, set to true when the first change is processed and a BEGIN message is transmitted.

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - [pgoutput_begin_txn](../p/pgoutput_begin_txn.md)
  - [pgoutput_send_begin](../p/pgoutput_send_begin.md)
  - [pgoutput_commit_txn](../p/pgoutput_commit_txn.md)
  - [pgoutput_change](../p/pgoutput_change.md)
  - [pgoutput_truncate](../p/pgoutput_truncate.md)
  - [pgoutput_message](../p/pgoutput_message.md)

## Notes and Other Information
- This optimization specifically targets empty transactions to reduce network overhead in logical replication
- The optimization is intentionally disabled for prepared transactions to prevent protocol inconsistencies during WALSender restarts
- Streamed transactions also bypass this optimization due to their potential inclusion of prepared transactions
- The design reflects PostgreSQL's emphasis on correctness over performance optimization in edge cases involving complex transaction states
- This is a simple but effective optimization that can significantly reduce network traffic in workloads with many empty transactions