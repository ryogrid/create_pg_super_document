# AddBufferToRing

## Location
[src/backend/storage/buffer/freelist.c:748-757](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L748-L757)

## Overview
Adds a buffer to the current slot in a ring-based buffer access strategy, completing the ring buffer assignment after allocation.

## Definition
```c
static void AddBufferToRing(BufferAccessStrategy strategy, BufferDesc *buf)
```

## Detailed Description
AddBufferToRing is a static function that adds a newly allocated or selected buffer to the current position in a ring-based buffer access strategy. This function is called after a buffer has been allocated through the normal buffer allocation process when GetBufferFromRing returned NULL (indicating no suitable buffer was available in the ring).

The function is extremely simple by design since it's called while holding the buffer header spinlock, requiring it to be very fast. It simply stores the buffer number (obtained via BufferDescriptorGetBuffer) in the current slot of the strategy's buffer array.

This function works in conjunction with GetBufferFromRing to maintain the ring structure: GetBufferFromRing advances the current position and tries to reuse an existing buffer, and if that fails, AddBufferToRing is used to populate the current slot with a newly allocated buffer.

## Parameters / Member Variables
- `strategy`: BufferAccessStrategy - The buffer access strategy whose ring should be updated
- `buf`: BufferDesc* - The buffer descriptor to add to the ring at the current position

## Dependencies
- Functions called/Symbols referenced:
  - BufferAccessStrategy (type)
  - BufferDesc (type)
  - BufferDescriptorGetBuffer (function)
- Called from (representative examples):
  - StrategyGetBuffer (multiple call sites)

## Notes and Other Information
- Static function, only called internally within buffer freelist management
- Must be called with the buffer header spinlock held, so implementation is kept minimal for performance
- Works as the complement to GetBufferFromRing in managing ring buffer allocation
- Simply assigns the buffer number to the current slot in the strategy's buffer array
- Critical for populating ring slots when existing buffers cannot be reused
- The current position is managed by GetBufferFromRing, so this function doesn't modify strategy->current