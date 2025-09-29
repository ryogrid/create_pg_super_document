# BufferIsPermanent

## Location
[src/backend/storage/buffer/bufmgr.c:3944-3973](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3944-L3973)

## Overview
BufferIsPermanent determines whether a buffer will potentially still be around after a crash, checking if the buffer corresponds to permanent (non-temporary) data.

## Definition

```c
bool
BufferIsPermanent(Buffer buffer)
```
## Detailed Description
BufferIsPermanent is a buffer management function that checks whether a given buffer is associated with permanent storage that will survive a database crash. The function first validates that the buffer is not a local buffer (which are used only for temporary relations) and then examines the buffer's state flags to determine if it has the BM_PERMANENT flag set. The function is designed to be safe to call while holding a buffer pin, as it performs atomic reads of the buffer state without requiring spinlock acquisition.

## Parameters / Member Variables
- : The Buffer identifier to check for permanence

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal
  - BufferIsPinned (assertion only)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - BM_PERMANENT (flag constant)
  - [BufferDesc](BufferDesc.md) (type)
- Called from (representative examples):
  - [SetHintBits](../S/SetHintBits.md)
  - RelationGetNumberOfBlocks

## Notes and Other Information
- Caller must hold a buffer pin on the buffer being checked
- Local buffers are always considered non-permanent as they are used only for temporary relations
- The function performs atomic reads of buffer state, making it safe to call concurrently
- The BM_PERMANENT flag cannot change while a pin is held, eliminating the need for spinlock protection
- Used primarily in visibility and hint bit operations where permanence affects caching decisions

## Simplified Source

```c
bool
BufferIsPermanent(Buffer buffer)
{
    BufferDesc *buf_hdr;

    // Local buffers are used only for temp relations
    if (BufferIsLocal(buffer))
        return false;

    // Verify buffer validity and pin
    Assert(BufferIsValid(buffer));
    Assert(BufferIsPinned(buffer));

    // Check BM_PERMANENT flag atomically (no spinlock needed while pinned)
    buf_hdr = GetBufferDescriptor(buffer - 1);
    return (pg_atomic_read_u32(&buf_hdr->state) & BM_PERMANENT) != 0;
}
```