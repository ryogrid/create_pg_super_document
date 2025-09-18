# AtEOSubXact_Inval

## Location
src/backend/utils/cache/inval.c: 1085 - 1172

## Overview
Processes queued invalidation messages at the end of a subtransaction, handling message propagation to parent transactions on commit or local processing on abort.

## Definition


## Detailed Description
This function manages invalidation messages when a subtransaction completes, implementing different strategies based on whether the subtransaction commits or aborts.

On commit, the function first processes any remaining CurrentCmdInvalidMsgs via CommandEndInvalidationMessages(). It then implements an optimization: if the parent transaction doesn't have an invalidation stack entry at the immediate parent level, it simply adjusts the current entry's level rather than creating a new parent entry. Otherwise, it propagates invalidation messages to the parent's PriorCmdInvalidMsgs list and adjusts the parent's CurrentCmdInvalidMsgs indexes accordingly. Any pending relcache init file invalidation is also propagated to the parent.

On abort, the function only processes PriorCmdInvalidMsgs locally since the aborted changes won't be visible to other backends. CurrentCmdInvalidMsgs can be safely discarded.

The function uses GetCurrentTransactionNestLevel() to verify it's operating at the correct nesting level and includes safeguards against processing the same nesting level twice during abort scenarios.

## Parameters / Member Variables
- : Boolean flag indicating whether the subtransaction is committing (true) or aborting (false)

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionNestLevel
  - CommandEndInvalidationMessages
  - AppendInvalidationMessages
  - SetGroupToFollow
  - ProcessInvalidationMessages
  - LocalExecuteInvalidationMessage
- Called from (representative examples):
  - CommitSubTransaction
  - AbortSubTransaction

## Notes and Other Information
- Quick exits if no invalidation messages exist or if messages aren't for the current transaction level
- Implements lazy creation of invalidation stack entries for optimization
- Uses level adjustment rather than data copying when possible to improve performance
- Propagates relcache init file invalidation flags to parent transactions on commit
- Includes protection against double-processing the same nesting level during aborts
- Memory cleanup relies on CurTransactionContext destruction rather than explicit freeing
- Messages passed to parent are placed in PriorCmdInvalidMsgs since they've already been locally processed
- The function maintains proper invalidation stack discipline by popping completed transaction levels