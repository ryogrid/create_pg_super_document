# PreventInTransactionBlock

## Location
[src/backend/access/transam/xact.c:3584-3655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3584-L3655)

## Overview
PreventInTransactionBlock ensures that specific SQL statements that cannot run safely within transaction blocks are rejected, preventing potential data corruption or inconsistency from non-rollback-able operations.

## Definition


## Detailed Description
This function serves as a critical safety mechanism for PostgreSQL statements that have non-rollback-able side effects or perform internal commits. It performs comprehensive checks to ensure the statement is not running in contexts where its effects could not be properly managed within the transaction system.

The function validates that the statement is not executing within a transaction block, subtransaction, pipeline, or user-defined function. If any of these conditions are violated, it raises an appropriate error. When all checks pass, it sets the XACT_FLAGS_NEEDIMMEDIATECOMMIT flag to ensure postgres.c commits the transaction immediately after statement completion.

## Parameters / Member Variables
- : bool - indicates whether the statement is being executed at the top level (not inside a function)
- : const char* - name of the statement type for error message formatting

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionBlock](../I/IsTransactionBlock.md)
  - [IsSubTransaction](../I/IsSubTransaction.md)
  - ereport (for error reporting)
  - elog (for fatal errors)
  - CurrentTransactionState (global variable)
  - MyXactFlags (global variable)
- Constants referenced:
  - XACT_FLAGS_PIPELINING
  - XACT_FLAGS_NEEDIMMEDIATECOMMIT
  - TBLOCK_DEFAULT
  - TBLOCK_STARTED
  - ERROR, FATAL (error levels)
  - ERRCODE_ACTIVE_SQL_TRANSACTION
- Called from (representative examples):
  - [cluster](../c/cluster.md) (CLUSTER command)
  - [AlterDatabase](../A/AlterDatabase.md)
  - [DiscardAll](../D/DiscardAll.md) (DISCARD ALL command)
  - [ExecReindex](../E/ExecReindex.md) (REINDEX commands)
  - [CreateSubscription](../C/CreateSubscription.md), DropSubscription (subscription management)
  - vacuum (VACUUM command)
  - Various replication commands
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (multiple statement types)

## Notes and Other Information
This function is essential for maintaining PostgreSQL's ACID properties by preventing statements with non-transactional effects from running in transactional contexts. Common commands that use this include VACUUM, CLUSTER, REINDEX, subscription commands, and database modification commands. The immediate commit flag ensures these operations are committed as soon as they complete successfully, providing the atomicity guarantee these statements require.