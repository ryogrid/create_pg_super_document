# RememberToFreeTupleDescAtEOX

## Location
[src/backend/utils/cache/relcache.c:3115-3143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L3115-L3143)

## Overview
Registers a TupleDesc for deferred cleanup at end-of-transaction, managing memory allocation to prevent immediate deallocation when the TupleDesc might still be in use.

## Definition
```c
static void RememberToFreeTupleDescAtEOX(TupleDesc td)
```

## Detailed Description
RememberToFreeTupleDescAtEOX is a memory management function that defers the cleanup of TupleDesc structures until the end of the current transaction (EOX - End Of Transaction). This is necessary because TupleDescs might still be referenced by other parts of the system even after a relation cache entry is destroyed.

The function maintains a dynamically growing array (EOXactTupleDescArray) to store TupleDesc pointers that need to be freed later. The array is allocated in CacheMemoryContext to ensure it persists for the duration of the transaction.

Key behaviors:
1. **Initial allocation**: Creates a 16-element array on first use
2. **Dynamic growth**: Doubles the array size when it becomes full using repalloc
3. **Registration**: Adds the TupleDesc pointer to the array and increments the counter

The actual cleanup of these TupleDescs happens during transaction cleanup, ensuring safe memory management in PostgreSQL's multi-layered caching system.

## Parameters / Member Variables
- `td`: The TupleDesc to be remembered for deferred cleanup at end-of-transaction

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
- Called from (representative examples):
  - [RelationDestroyRelation](RelationDestroyRelation.md)

## Notes and Other Information
- This is a static function, only accessible within the relcache.c module
- The array is allocated in CacheMemoryContext to ensure proper memory lifecycle management
- Uses a doubling strategy for array growth to minimize reallocation overhead
- Part of PostgreSQL's sophisticated memory management system for relation cache cleanup
- The deferred cleanup pattern prevents use-after-free errors when TupleDescs might still be referenced elsewhere in the system
- The "EOX" prefix stands for "End Of Transaction" indicating when the cleanup will occur