# AtEOSubXact_Inval

## Location
[src/backend/utils/cache/inval.c:1085-1172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1085-L1172)

## Overview
Processes queued invalidation messages at the end of a subtransaction, handling message propagation to parent transactions on commit or local processing on abort.

## Definition

```c
void
AtEOSubXact_Inval(bool isCommit)
```
## Detailed Description
This function manages invalidation messages when a subtransaction completes, implementing different strategies based on whether the subtransaction commits or aborts.

On commit, the function first processes any remaining CurrentCmdInvalidMsgs via CommandEndInvalidationMessages(). It then implements an optimization: if the parent transaction doesn't have an invalidation stack entry at the immediate parent level, it simply adjusts the current entry's level rather than creating a new parent entry. Otherwise, it propagates invalidation messages to the parent's PriorCmdInvalidMsgs list and adjusts the parent's CurrentCmdInvalidMsgs indexes accordingly. Any pending relcache init file invalidation is also propagated to the parent.

On abort, the function only processes PriorCmdInvalidMsgs locally since the aborted changes won't be visible to other backends. CurrentCmdInvalidMsgs can be safely discarded.

The function uses GetCurrentTransactionNestLevel() to verify it's operating at the correct nesting level and includes safeguards against processing the same nesting level twice during abort scenarios.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether the subtransaction is committing (true) or aborting (false)
## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [CommandEndInvalidationMessages](../C/CommandEndInvalidationMessages.md)
  - [AppendInvalidationMessages](AppendInvalidationMessages.md)
  - SetGroupToFollow
  - [ProcessInvalidationMessages](../P/ProcessInvalidationMessages.md)
  - [LocalExecuteInvalidationMessage](../L/LocalExecuteInvalidationMessage.md)
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md)
  - [AbortSubTransaction](AbortSubTransaction.md)

## Notes and Other Information
- Quick exits if no invalidation messages exist or if messages aren't for the current transaction level
- Implements lazy creation of invalidation stack entries for optimization
- Uses level adjustment rather than data copying when possible to improve performance
- Propagates relcache init file invalidation flags to parent transactions on commit
- Includes protection against double-processing the same nesting level during aborts
- Memory cleanup relies on CurTransactionContext destruction rather than explicit freeing
- Messages passed to parent are placed in PriorCmdInvalidMsgs since they've already been locally processed
- The function maintains proper invalidation stack discipline by popping completed transaction levels

## Simplified Source

```c
// Simplified version of AtEOSubXact_Inval
void AtEOSubXact_Inval(bool isCommit) {
    int my_level;
    TransInvalidationInfo *myInfo = transInvalInfo;

    // Quick exit if no invalidation messages exist
    if (myInfo == NULL)
        return;

    // Verify we're processing the correct transaction level
    my_level = GetCurrentTransactionNestLevel();
    if (myInfo->my_level != my_level) {
        // Messages aren't for this level - bail out
        return;
    }

    if (isCommit) {
        // Process any remaining current command messages
        CommandEndInvalidationMessages();

        // Optimization: adjust level instead of creating parent entry
        if (myInfo->parent == NULL || myInfo->parent->my_level < my_level - 1) {
            myInfo->my_level--;
            return;
        }

        // Pass invalidation messages up to parent transaction
        AppendInvalidationMessages(&myInfo->parent->PriorCmdInvalidMsgs,
                                 &myInfo->PriorCmdInvalidMsgs);

        // Update parent's message indexes
        SetGroupToFollow(&myInfo->parent->CurrentCmdInvalidMsgs,
                        &myInfo->parent->PriorCmdInvalidMsgs);

        // Propagate relcache invalidation flag to parent
        if (myInfo->RelcacheInitFileInval)
            myInfo->parent->RelcacheInitFileInval = true;

        // Pop transaction stack and cleanup
        transInvalInfo = myInfo->parent;
        pfree(myInfo);
    }
    else {
        // Abort case: process messages locally only
        ProcessInvalidationMessages(&myInfo->PriorCmdInvalidMsgs,
                                  LocalExecuteInvalidationMessage);

        // Pop transaction stack and cleanup
        transInvalInfo = myInfo->parent;
        pfree(myInfo);
    }
}
```

Key simplifications made:
- Removed detailed comments explaining implementation rationale
- Simplified variable declarations to essential ones only
- Consolidated assertion checks into the main logic flow
- Abstracted complex invalidation message handling with descriptive comments
- Removed defensive programming comments about memory management
- Streamlined the commit/abort branching logic for clarity
- Maintained all essential algorithm steps and data structure operations