# xactGetCommittedInvalidationMessages

## Location
[src/backend/utils/cache/inval.c:883-961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L883-L961)

## Overview
Collects all invalidation messages from the current transaction for inclusion in the commit record before the transaction officially commits.

## Definition

```c
int
xactGetCommittedInvalidationMessages(SharedInvalidationMessage **msgs,
									 bool *RelcacheInitFileInval)
```
## Detailed Description
xactGetCommittedInvalidationMessages is called by RecordTransactionCommit() to gather all invalidation messages that need to be included in the transaction's commit WAL record. This function runs before the transaction has officially committed and before AtEOXact_Inval() is called, ensuring it can access all the invalidation data that will be cleaned up later.

The function collects messages from both PriorCmdInvalidMsgs and CurrentCmdInvalidMsgs, organizing them into a single contiguous array in the same order that AtEOXact_Inval() would process them. This ensures that WAL replay will behave identically to the original transaction execution.

The function processes messages in a specific order:
1. Prior command catalog cache messages
2. Current command catalog cache messages  
3. Prior command relation cache messages
4. Current command relation cache messages

This ordering maintains consistency between original execution and replay scenarios.

## Parameters / Member Variables
- : Output parameter that receives a pointer to the array of invalidation messages
- : Output parameter indicating whether relation cache init file invalidation is required

## Dependencies
- Functions called/Symbols referenced:
  - NumMessagesInGroup
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - ProcessMessageSubGroupMulti
  - [SharedInvalidationMessage](../S/SharedInvalidationMessage.md) (type)
- Called from (representative examples):
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [StartPrepare](../S/StartPrepare.md)

## Notes and Other Information
- Must always run before AtEOXact_Inval() since that function cleans up the data needed here
- Returns the number of messages collected in the array
- Runs before official transaction commit, so must not change transaction outcome
- Memory is allocated in CurTransactionContext which will be cleaned up automatically
- Critical for WAL-based replication and recovery - ensures invalidation messages are durably recorded
- Used in both regular commits and two-phase commit preparation
- The message ordering is carefully maintained to ensure identical behavior during replay

## Simplified Source

```c
int
xactGetCommittedInvalidationMessages(SharedInvalidationMessage **msgs,
                                   bool *RelcacheInitFileInval)
{
    SharedInvalidationMessage *msgarray;
    int nummsgs;
    int nmsgs;

    // Quick exit if no invalidation messages
    if (transInvalInfo == NULL)
    {
        *RelcacheInitFileInval = false;
        *msgs = NULL;
        return 0;
    }

    // Must be at top level transaction
    Assert(transInvalInfo->my_level == 1 && transInvalInfo->parent == NULL);

    // Set relcache init file invalidation flag
    *RelcacheInitFileInval = transInvalInfo->RelcacheInitFileInval;

    // Count total messages needed
    nummsgs = NumMessagesInGroup(&transInvalInfo->PriorCmdInvalidMsgs) +
              NumMessagesInGroup(&transInvalInfo->CurrentCmdInvalidMsgs);

    // Allocate array for all messages
    *msgs = msgarray = (SharedInvalidationMessage *)
        MemoryContextAlloc(CurTransactionContext,
                          nummsgs * sizeof(SharedInvalidationMessage));

    // Copy messages in processing order to maintain consistency
    nmsgs = 0;

    // Prior catalog cache messages
    ProcessMessageSubGroupMulti(&transInvalInfo->PriorCmdInvalidMsgs,
                               CatCacheMsgs,
                               (memcpy(msgarray + nmsgs, msgs,
                                      n * sizeof(SharedInvalidationMessage)),
                                nmsgs += n));

    // Current catalog cache messages
    ProcessMessageSubGroupMulti(&transInvalInfo->CurrentCmdInvalidMsgs,
                               CatCacheMsgs,
                               (memcpy(msgarray + nmsgs, msgs,
                                      n * sizeof(SharedInvalidationMessage)),
                                nmsgs += n));

    // Prior relation cache messages
    ProcessMessageSubGroupMulti(&transInvalInfo->PriorCmdInvalidMsgs,
                               RelCacheMsgs,
                               (memcpy(msgarray + nmsgs, msgs,
                                      n * sizeof(SharedInvalidationMessage)),
                                nmsgs += n));

    // Current relation cache messages
    ProcessMessageSubGroupMulti(&transInvalInfo->CurrentCmdInvalidMsgs,
                               RelCacheMsgs,
                               (memcpy(msgarray + nmsgs, msgs,
                                      n * sizeof(SharedInvalidationMessage)),
                                nmsgs += n));

    Assert(nmsgs == nummsgs);
    return nmsgs;
}
```