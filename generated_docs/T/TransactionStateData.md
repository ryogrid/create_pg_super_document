# TransactionStateData

## Location
[src/backend/access/transam/xact.c:191-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L191-L216)

## Overview
TransactionStateData is a core structure that maintains the complete state information for a PostgreSQL transaction, including nested subtransactions, savepoints, and parallel execution context.

## Definition

```c
typedef struct TransactionStateData
{
	FullTransactionId fullTransactionId;	/* my FullTransactionId */
	SubTransactionId subTransactionId;	/* my subxact ID */
	char	   *name;			/* savepoint name, if any */
	int			savepointLevel; /* savepoint level */
	TransState	state;			/* low-level state */
	TBlockState blockState;		/* high-level state */
	int			nestingLevel;	/* transaction nesting depth */
	int			gucNestLevel;	/* GUC context nesting depth */
	MemoryContext curTransactionContext;	/* my xact-lifetime context */
	ResourceOwner curTransactionOwner;	/* my query resources */
	TransactionId *childXids;	/* subcommitted child XIDs, in XID order */
	int			nChildXids;		/* # of subcommitted child XIDs */
	int			maxChildXids;	/* allocated size of childXids[] */
	Oid			prevUser;		/* previous CurrentUserId setting */
	int			prevSecContext; /* previous SecurityRestrictionContext */
	bool		prevXactReadOnly;	/* entry-time xact r/o state */
	bool		startedInRecovery;	/* did we start in recovery? */
	bool		didLogXid;		/* has xid been included in WAL record? */
	int			parallelModeLevel;	/* Enter/ExitParallelMode counter */
	bool		parallelChildXact;	/* is any parent transaction parallel? */
	bool		chain;			/* start a new block after this one */
	bool		topXidLogged;	/* for a subxact: is top-level XID logged? */
	struct TransactionStateData *parent;	/* back link to parent */
} TransactionStateData;
```
## Detailed Description
TransactionStateData serves as the comprehensive state container for PostgreSQL's transaction management system. It tracks both low-level transaction states and high-level block states, manages nested subtransactions through a parent-child relationship, and maintains context for parallel execution modes. The structure supports PostgreSQL's sophisticated transaction nesting capabilities, including savepoints, and ensures proper resource management across transaction boundaries.

## Parameters / Member Variables
- `fullTransactionId`: The complete transaction identifier for this transaction
- `subTransactionId`: Identifier for subtransaction within the main transaction
- `*name`: Optional savepoint name for named savepoints
- `savepointLevel`: Numeric level indicating savepoint nesting depth
- `state`: Low-level transaction state (TransState enum)
- `blockState`: High-level transaction block state (TBlockState enum)
- `nestingLevel`: Depth of transaction nesting
- `gucNestLevel`: GUC (Grand Unified Configuration) context nesting level
- `curTransactionContext`: Memory context specific to this transaction's lifetime
- `curTransactionOwner`: Resource owner managing query-level resources
- `*childXids`: Array of committed child transaction IDs, maintained in XID order
- `nChildXids`: Current number of child transaction IDs in the array
- `maxChildXids`: Allocated capacity of the childXids array
- `prevUser`: Previous user ID before transaction started
- `prevSecContext`: Previous security restriction context
- `prevXactReadOnly`: Read-only state when transaction began
- `startedInRecovery`: Flag indicating if transaction started during recovery
- `didLogXid`: Whether transaction ID has been written to WAL
- `parallelModeLevel`: Counter for Enter/ExitParallelMode calls
- `parallelChildXact`: Whether any parent transaction is executing in parallel mode
- `chain`: Flag to automatically start new transaction block after current one
- `topXidLogged`: For subtransactions, whether top-level XID is logged
- `*parent`: Back-reference to parent transaction state for nested transactions
## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionId](../F/FullTransactionId.md)
  - SubTransactionId
  - TransState
  - TBlockState
  - [ResourceOwner](../R/ResourceOwner.md)
- Called from (representative examples):
  - TransactionState (typedef)
  - SerializedTransactionStateHeaderSize
  - [PushTransaction](../P/PushTransaction.md)

## Notes and Other Information
The parallelModeLevel field counts unmatched EnterParallelMode calls at this transaction level, while parallelChildXact tracks if any upper transaction level has nonzero parallelModeLevel. This design enables proper parallel execution context management across nested transactions. The structure forms a linked list through the parent pointer, allowing PostgreSQL to maintain a complete transaction stack for proper rollback and resource cleanup operations.