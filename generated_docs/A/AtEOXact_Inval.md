# AtEOXact_Inval

## Location
src/backend/utils/cache/inval.c: 1026 - 1084

## Overview
Processes queued invalidation messages at the end of a main transaction, handling both commit and abort scenarios with appropriate message distribution.

## Definition


## Detailed Description
This function is called as the last step in processing a transaction to handle all queued invalidation messages. It implements different behavior based on whether the transaction is committing or aborting.

On commit, the function consolidates messages from PriorCmdInvalidMsgs and CurrentCmdInvalidMsgs, then sends them to the shared invalidation message queue where they will be read by other backends and by our own backend at the next transaction start. For relcache init file invalidation, it performs both pre- and post-invalidation processing around the message sending.

On abort, the function only processes PriorCmdInvalidMsgs locally since other backends haven't seen the aborted changes. CurrentCmdInvalidMsgs can be safely ignored as those changes haven't touched the caches yet.

After processing, the function resets the invalidation state to empty, preparing for the next transaction. Memory cleanup is handled by TopTransactionContext destruction.

## Parameters / Member Variables
- : Boolean flag indicating whether the transaction is committing (true) or aborting (false)

## Dependencies
- Functions called/Symbols referenced:
  - RelationCacheInitFilePreInvalidate
  - AppendInvalidationMessages
  - ProcessInvalidationMessagesMulti
  - SendSharedInvalidMessages
  - RelationCacheInitFilePostInvalidate
  - ProcessInvalidationMessages
  - LocalExecuteInvalidationMessage
- Called from (representative examples):
  - CommitTransaction
  - AbortTransaction
  - PostPrepare_Inval

## Notes and Other Information
- Must be called at the top level of the transaction stack (level 1 with no parent)
- Quick exits if no invalidation messages are queued (transInvalInfo == NULL)
- Includes an injection point for testing transaction-end invalidation processing
- Does not explicitly free memory as TopTransactionContext cleanup handles this
- The function implements the invalidation protocol ensuring cache consistency across backends
- Relcache init file invalidation requires careful ordering of pre- and post-processing steps
- On commit, messages are shared with other backends; on abort, only local processing occurs