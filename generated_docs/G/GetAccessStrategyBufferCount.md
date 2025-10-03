# GetAccessStrategyBufferCount

## Location
[src/backend/storage/buffer/freelist.c:624-646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L624-L646)

## Overview
Returns the number of buffers in a buffer access strategy ring, providing access to the ring size for monitoring and configuration purposes.

## Definition

```c
int
GetAccessStrategyBufferCount(BufferAccessStrategy strategy)
```
## Detailed Description
GetAccessStrategyBufferCount is an accessor function that returns the number of buffers configured in a buffer access strategy ring. This function provides a safe way to query the size of the buffer ring without directly accessing the strategy structure members. The function handles NULL input gracefully by returning 0, which matches the behavior of GetAccessStrategyWithSize() returning NULL when given a size of 0.

This function is primarily used for monitoring buffer strategy usage and for making decisions about parallel operations that need to know the buffer ring capacity.

## Parameters / Member Variables
- `strategy`: BufferAccessStrategy - The buffer access strategy whose buffer count should be returned. Can be NULL.
## Dependencies
- Functions called/Symbols referenced:
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md) (type)
- Called from (representative examples):
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md)
  - RelationGetNumberOfBlocks

## Notes and Other Information
- Returns 0 when strategy is NULL, providing consistent behavior with other access strategy functions
- The returned value represents the nbuffers field of the strategy structure
- Used primarily in vacuum operations and relation block counting scenarios
- Part of the buffer access strategy API that provides controlled buffer ring management