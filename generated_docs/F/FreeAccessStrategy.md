# FreeAccessStrategy

## Location
[src/backend/storage/buffer/freelist.c:681-694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L681-L694)

## Overview
Releases memory allocated for a BufferAccessStrategy object, providing safe cleanup that handles NULL pointers gracefully.

## Definition
```c
void FreeAccessStrategy(BufferAccessStrategy strategy)
```

## Detailed Description
FreeAccessStrategy is the cleanup function for BufferAccessStrategy objects, responsible for releasing the memory allocated to a buffer access strategy. The function encapsulates the memory deallocation details and provides a stable API that doesn't expose the internal representation of the strategy structure.

The function safely handles NULL input by checking the strategy pointer before attempting to free it, preventing crashes when called on "default" strategies (which are represented as NULL pointers). This design allows callers to uniformly call FreeAccessStrategy regardless of whether they're using a custom strategy or the default buffer management behavior.

## Parameters / Member Variables
- `strategy`: BufferAccessStrategy - The buffer access strategy to be freed. Can be NULL (no operation performed).

## Dependencies
- Functions called/Symbols referenced:
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md) (type)
  - [pfree](../p/pfree.md) (memory deallocation function)
- Called from (representative examples):
  - [initscan](../i/initscan.md)
  - [heap_endscan](../h/heap_endscan.md)
  - [FreeBulkInsertState](FreeBulkInsertState.md)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md)
  - [RelationCopyStorageUsingBuffer](../R/RelationCopyStorageUsingBuffer.md)
  - RelationGetNumberOfBlocks

## Notes and Other Information
- Safely handles NULL pointers, preventing crashes when called on default strategies
- Currently implemented as a simple pfree, but the API abstraction allows for future changes to the internal representation
- Commonly used in cleanup paths of heap scans, bulk operations, and vacuum processes
- Part of the buffer access strategy lifecycle management alongside GetAccessStrategyWithSize()
- Essential for preventing memory leaks in long-running operations that use custom buffer strategies