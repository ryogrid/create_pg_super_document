# BufferIsPermanent

## Location
src/backend/storage/buffer/bufmgr.c: 3944 - 3973

## Overview
BufferIsPermanent determines whether a buffer will potentially still be around after a crash, checking if the buffer corresponds to permanent (non-temporary) data.

## Definition


## Detailed Description
BufferIsPermanent is a buffer management function that checks whether a given buffer is associated with permanent storage that will survive a database crash. The function first validates that the buffer is not a local buffer (which are used only for temporary relations) and then examines the buffer's state flags to determine if it has the BM_PERMANENT flag set. The function is designed to be safe to call while holding a buffer pin, as it performs atomic reads of the buffer state without requiring spinlock acquisition.

## Parameters / Member Variables
- : The Buffer identifier to check for permanence

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal
  - BufferIsPinned (assertion only)
  - GetBufferDescriptor
  - pg_atomic_read_u32
  - BM_PERMANENT (flag constant)
  - BufferDesc (type)
- Called from (representative examples):
  - SetHintBits
  - RelationGetNumberOfBlocks

## Notes and Other Information
- Caller must hold a buffer pin on the buffer being checked
- Local buffers are always considered non-permanent as they are used only for temporary relations
- The function performs atomic reads of buffer state, making it safe to call concurrently
- The BM_PERMANENT flag cannot change while a pin is held, eliminating the need for spinlock protection
- Used primarily in visibility and hint bit operations where permanence affects caching decisions