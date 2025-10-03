# PrepareInvalidationState

## Location
[src/backend/utils/cache/inval.c:612-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L612-L674)

## Overview
Initializes invalidation data structures for the current transaction or subtransaction, managing the hierarchical invalidation state stack.

## Definition
```c
static void PrepareInvalidationState(void)
```

## Detailed Description
PrepareInvalidationState is a static function that sets up the invalidation state infrastructure for a transaction or subtransaction. It manages a stack of TransInvalidationInfo structures, with each level corresponding to a transaction nesting level (main transaction, savepoints, subtransactions).

Key behaviors:
1. **Idempotency**: Returns early if invalidation state already exists for the current transaction nesting level
2. **Memory Management**: Allocates TransInvalidationInfo in TopTransactionContext to ensure it persists for the transaction lifetime
3. **Hierarchy Management**: Links the new state to its parent transaction state, maintaining proper nesting relationships
4. **Message Array Setup**: For nested transactions, properly positions message array indices to follow the parent's arrays
5. **Validation**: Ensures parent transactions have no unprocessed messages before starting subtransactions, preventing semantic inconsistencies

The function ensures that invalidation messages are properly scoped and processed according to transaction boundaries, maintaining cache consistency across transaction rollbacks and commits.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [TransInvalidationInfo](../T/TransInvalidationInfo.md) (struct type)
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - NumMessagesInGroup
  - SetGroupToFollow
  - CatCacheMsgs
  - RelCacheMsgs
- Called from (representative examples):
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
  - [CacheInvalidateCatalog](../C/CacheInvalidateCatalog.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [CacheInvalidateRelcacheAll](../C/CacheInvalidateRelcacheAll.md)
  - [CacheInvalidateRelcacheByTuple](../C/CacheInvalidateRelcacheByTuple.md)
  - [CacheInvalidateRelcacheByRelid](../C/CacheInvalidateRelcacheByRelid.md)

## Notes and Other Information
- This is a static function internal to the invalidation system
- Manages the global transInvalInfo pointer that tracks current invalidation state
- Critical for proper transaction and subtransaction semantics in cache invalidation
- Prevents cache consistency issues by enforcing that parent transactions have no pending messages when subtransactions start
- The message array management ensures that invalidation messages are properly partitioned by transaction level
- Uses TopTransactionContext to ensure invalidation state survives for the entire transaction

## Simplified Source

```c
static void PrepareInvalidationState(void)
{
    TransInvalidationInfo *myInfo;

    // Check if we already have state for current transaction level
    if (transInvalInfo != NULL &&
        transInvalInfo->my_level == GetCurrentTransactionNestLevel())
        return;

    // Allocate new invalidation state in transaction context
    myInfo = (TransInvalidationInfo *)
        MemoryContextAllocZero(TopTransactionContext, sizeof(TransInvalidationInfo));
    myInfo->parent = transInvalInfo;
    myInfo->my_level = GetCurrentTransactionNestLevel();

    // Handle nested transaction setup
    if (transInvalInfo != NULL) {
        // Verify we're creating a deeper nesting level
        Assert(myInfo->my_level > transInvalInfo->my_level);

        // Parent must not have unprocessed messages
        if (NumMessagesInGroup(&transInvalInfo->CurrentCmdInvalidMsgs) != 0)
            elog(ERROR, "cannot start a subtransaction when there are unprocessed inval messages");

        // Set up message array indices to follow parent arrays
        SetGroupToFollow(&myInfo->PriorCmdInvalidMsgs, &transInvalInfo->CurrentCmdInvalidMsgs);
        SetGroupToFollow(&myInfo->CurrentCmdInvalidMsgs, &myInfo->PriorCmdInvalidMsgs);
    } else {
        // Clear any leftover array pointers from prior transaction
        InvalMessageArrays[CatCacheMsgs].msgs = NULL;
        InvalMessageArrays[CatCacheMsgs].maxmsgs = 0;
        InvalMessageArrays[RelCacheMsgs].msgs = NULL;
        InvalMessageArrays[RelCacheMsgs].maxmsgs = 0;
    }

    // Set as current invalidation state
    transInvalInfo = myInfo;
}
```