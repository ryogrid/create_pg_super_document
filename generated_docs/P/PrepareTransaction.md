# PrepareTransaction

## Location
[src/backend/access/transam/xact.c:2460-2748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L2460-L2748)

## Overview
PrepareTransaction implements the first phase of PostgreSQL's two-phase commit protocol, preparing a transaction for later commit or rollback while maintaining its state in persistent storage.

## Definition

```c
static void
PrepareTransaction(void)
```
## Detailed Description
PrepareTransaction executes the "prepare" phase of a two-phase commit, which involves saving the transaction's state to persistent storage while keeping it uncommitted. This allows the transaction to survive system crashes and be later committed or rolled back by another process.

The function performs comprehensive validation and preparation:
- Executes the same pre-commit processing as CommitTransaction (triggers, portal cleanup)
- Validates that the transaction doesn't use temporary objects or exported snapshots (both are incompatible with two-phase commit)
- Records the transaction state using the two-phase commit infrastructure
- Transfers locks and resources from the current backend to a dummy PGPROC entry
- Detaches the transaction from the current backend while keeping it alive globally

The transaction transitions through states: TRANS_INPROGRESS → TRANS_PREPARE → TRANS_DEFAULT, but remains globally active in the prepared state.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : The current transaction's state structure  
- : Transaction ID obtained via GetCurrentTransactionId()
- : Global transaction entry created by MarkAsPreparing()
- : Global identifier for the prepared transaction (from external context)
- : Timestamp when preparation occurred

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md) (obtains the current XID)
  - [MarkAsPreparing](../M/MarkAsPreparing.md) (reserves GID and creates global transaction entry)
  - [StartPrepare](../S/StartPrepare.md)/EndPrepare (manages two-phase state file creation)
  - AtPrepare_* functions (collect data for two-phase state file)
  - PostPrepare_* functions (clean up after preparation)
  - [PostPrepare_Twophase](PostPrepare_Twophase.md) (completes transaction detachment)
  - [ProcArrayClearTransaction](ProcArrayClearTransaction.md) (removes from process array)
  
- Called from (representative examples):
  - [CommitTransactionCommandInternal](../C/CommitTransactionCommandInternal.md) (when processing PREPARE TRANSACTION command)

## Notes and Other Information
- Contains the same warning as CommitTransaction about coordinating changes between the two functions
- Explicitly prohibits preparing transactions that accessed temporary objects or exported snapshots
- Uses the same pre-commit loop as CommitTransaction to handle triggers and portals
- Resource cleanup follows a specific order to ensure proper two-phase commit semantics
- The prepared transaction can survive backend termination and system crashes
- After PostPrepare_Twophase(), the transaction is completely detached from the current backend
- Treats PREPARE like ROLLBACK for some subsystems (apply launcher, logical replication workers) since the transaction isn't yet committed