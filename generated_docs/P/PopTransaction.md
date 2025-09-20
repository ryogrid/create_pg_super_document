# PopTransaction

## Location
[src/backend/access/transam/xact.c:5416-5449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5416-L5449)

## Overview
PopTransaction removes the current subtransaction from the transaction state stack and restores the parent transaction as the current transaction state.

## Definition

```c
structure */
	if (s->name)
		pfree(s->name);
```
## Detailed Description
PopTransaction is a static function that pops the current subtransaction from the transaction state stack and restores the parent transaction context. The function performs the following operations:

1. **State Validation**: 
   - Checks that the current transaction state is TRANS_DEFAULT before proceeding
   - Ensures that a parent transaction exists (prevents popping the top-level transaction)
2. **Stack Management**: Updates CurrentTransactionState to point to the parent transaction
3. **Context Restoration**: 
   - Restores CurTransactionContext to the parent's transaction context
   - Switches the current memory context to the parent's context
4. **Resource Owner Restoration**: 
   - Restores CurTransactionResourceOwner and CurrentResourceOwner to the parent's resource owner
5. **Memory Cleanup**: 
   - Frees the subtransaction's name if it exists
   - Frees the subtransaction state structure

This function is the counterpart to PushTransaction and is called at the end of both successful subtransaction commits and failed subtransaction cleanups to restore the transaction hierarchy to its previous state.

## Parameters / Member Variables
This function takes no parameters and operates on the global CurrentTransactionState.

## Dependencies
- Functions called/Symbols referenced:
  - [TransStateAsString](../T/TransStateAsString.md)
  - elog (for warnings and fatal errors)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md)
  - [CleanupSubTransaction](../C/CleanupSubTransaction.md)

## Notes and Other Information
- The function includes a warning that callers must reassign CurrentTransactionState local pointers after calling this function
- Issues a WARNING if called while not in TRANS_DEFAULT state, indicating an unexpected transaction state
- Issues a FATAL error if called when there is no parent transaction, preventing corruption of the transaction stack
- The function performs both memory context and resource owner restoration to ensure complete context switching
- Memory cleanup includes freeing the optional transaction name if one was assigned
- The function is designed to be safe to call multiple times or in error conditions, with appropriate state validation
- This is the final step in subtransaction lifecycle management, completing the transaction stack unwinding
- Located in src/backend/access/transam/xact.c:5416-5449