# smgrDoPendingDeletes

## Location
src/backend/catalog/storage.c: 657 - 724

## Overview
smgrDoPendingDeletes executes relation file deletions that were deferred until transaction end, handling both commit and abort scenarios.

## Definition
```c
void smgrDoPendingDeletes(bool isCommit)
```

## Detailed Description
This function processes the list of pending relation deletions that were queued during the transaction. It operates at transaction boundaries (commit or abort) and also when aborting subtransactions to ensure immediate cleanup of failed operations. The function filters pending deletions based on the current transaction nesting level and whether the deletion should occur at commit or abort.

For each applicable deletion, it opens the relation using the storage manager, collects them into an array, and then performs batch unlinking using smgrdounlinkall(). This approach is more efficient than deleting relations one by one. The function handles cases where relations may have no physical storage, such as temporary relations that were already cleaned up by RemovePgTempFiles.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether this is being called at commit (true) or abort (false)

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionNestLevel
  - smgropen
  - smgrdounlinkall
  - smgrclose
  - palloc
  - repalloc
  - pfree
- Called from (representative examples):
  - CommitTransaction
  - AbortTransaction
  - AtSubAbort_smgr

## Notes and Other Information
- The function handles transaction nesting by only processing entries at the current or deeper nesting levels
- Uses dynamic array allocation that starts at size 8 and doubles when needed for efficient batch processing
- Entries are unlinked from the pending list before processing to avoid retry attempts on failure
- Can handle relations that have no physical storage without error
- The atCommit flag in PendingRelDelete determines whether deletion occurs at commit or abort