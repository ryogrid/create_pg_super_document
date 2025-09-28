# PushTransaction

## Location
[src/backend/access/transam/xact.c:5354-5415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5354-L5415)

## Overview
PushTransaction creates a new subtransaction state entry and pushes it onto the transaction state stack, initializing all necessary fields for a new subtransaction.

## Definition

```c
structure */
	if (s->name)
		pfree(s->name);
```
## Detailed Description
PushTransaction is a static function responsible for creating and initializing a new subtransaction state node in PostgreSQL's transaction management system. The function performs the following operations:

1. **Memory Allocation**: Allocates a new TransactionStateData structure in TopTransactionContext using MemoryContextAllocZero
2. **Subtransaction ID Assignment**: 
   - Increments the global currentSubTransactionId counter
   - Checks for counter wraparound and raises an error if the limit (2^32-1) is exceeded
   - Ensures each subtransaction has a unique identifier
3. **State Initialization**: Initializes the new subtransaction state with:
   - Invalid full transaction ID (assigned later)
   - Current subtransaction ID from the counter
   - Parent pointer to the current transaction state
   - Incremented nesting level
   - New GUC nesting level
   - Inherited savepoint level from parent
   - Initial state of TRANS_DEFAULT
   - Block state of TBLOCK_SUBBEGIN
4. **Context Preservation**: Captures current user ID, security context, and read-only status
5. **Parallel Processing Setup**: Initializes parallel mode level and inherits parallel child transaction status
6. **Stack Update**: Sets the new subtransaction as the CurrentTransactionState

The function ensures that AbortSubTransaction and CleanupSubTransaction can handle the subtransaction even if it doesn't yet have a transaction context, resource owner, or XID.

## Parameters / Member Variables
This function takes no parameters and operates on global transaction state variables.

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)
  - [pfree](../p/pfree.md) (on error path)
  - ereport, errcode, errmsg (on error path)
- Called from (representative examples):
  - [DefineSavepoint](../D/DefineSavepoint.md)
  - [BeginInternalSubTransaction](../B/BeginInternalSubTransaction.md)

## Notes and Other Information
- The function includes a warning that callers must reassign CurrentTransactionState local pointers after calling this function
- Uses TopTransactionContext for subtransaction state allocation to ensure persistence across memory context switches
- Implements a hard limit of 2^32-1 subtransactions per main transaction to prevent counter wraparound
- The function creates a minimal valid subtransaction state that can be safely handled by abort and cleanup functions
- Parallel processing flags are carefully managed to track whether the subtransaction is created within a parallel operation
- The topXidLogged flag is initialized to false, indicating that the top-level XID hasn't been logged yet
- Located in src/backend/access/transam/xact.c:5354-5415

## Simplified Source

```c
// Simplified version of PushTransaction
static void PushTransaction(void) {
    TransactionState p = CurrentTransactionState;
    TransactionState s;

    // Allocate new subtransaction state in TopTransactionContext
    s = (TransactionState) MemoryContextAllocZero(TopTransactionContext,
                                                  sizeof(TransactionStateData));

    // Assign unique subtransaction ID with wraparound check
    currentSubTransactionId += 1;
    if (currentSubTransactionId == InvalidSubTransactionId) {
        currentSubTransactionId -= 1;
        pfree(s);
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("cannot have more than 2^32-1 subtransactions in a transaction")));
    }

    // Initialize new subtransaction state
    s->fullTransactionId = InvalidFullTransactionId;  // assigned later
    s->subTransactionId = currentSubTransactionId;
    s->parent = p;
    s->nestingLevel = p->nestingLevel + 1;
    s->gucNestLevel = NewGUCNestLevel();
    s->savepointLevel = p->savepointLevel;
    s->state = TRANS_DEFAULT;
    s->blockState = TBLOCK_SUBBEGIN;

    // Preserve current context
    GetUserIdAndSecContext(&s->prevUser, &s->prevSecContext);
    s->prevXactReadOnly = XactReadOnly;
    s->startedInRecovery = p->startedInRecovery;

    // Setup parallel processing state
    s->parallelModeLevel = 0;
    s->parallelChildXact = (p->parallelModeLevel != 0 || p->parallelChildXact);
    s->topXidLogged = false;

    // Make this the current transaction state
    CurrentTransactionState = s;
}
```

Key simplifications made:
- Preserved the essential subtransaction creation logic
- Maintained the critical ID assignment and wraparound protection
- Kept all necessary state initialization
- Focused on the core subtransaction stack management
- Retained memory allocation in the correct context