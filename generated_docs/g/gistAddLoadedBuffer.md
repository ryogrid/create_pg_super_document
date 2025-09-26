# gistAddLoadedBuffer

## Location
src/backend/access/gist/gistbuildbuffers.c: 198 - 220

## Overview
gistAddLoadedBuffer adds a node buffer to the array that tracks buffers currently loaded in memory during GiST index construction.

## Definition

```c
static void
gistAddLoadedBuffer(GISTBuildBuffers *gfbb, GISTNodeBuffer *nodeBuffer)
```
## Detailed Description
This static function manages the loadedBuffers array, which keeps track of node buffers that currently have their pages loaded in memory. This tracking is essential for memory management during index construction, as it allows the system to know which buffers are consuming memory and may need to be written to temporary storage if memory becomes constrained.

The function implements dynamic array growth - when the array becomes full, it doubles in size using repalloc. This exponential growth strategy ensures efficient amortized performance for array expansion operations.

An important safety check prevents temporary buffers (those marked with isTemp flag) from being added to the tracking array, as these buffers have different lifecycle management requirements and are handled separately.

## Parameters / Member Variables
- : The GiST build buffers structure containing the loadedBuffers array and related metadata
- : The node buffer to be added to the tracking array (must not be a temporary buffer)

## Dependencies
- Functions called/Symbols referenced:
  - repalloc
- Called from (representative examples):
  - gistLoadNodeBuffer
  - gistPushItupToNodeBuffer

## Notes and Other Information
- Function is declared static, making it internal to the gistbuildbuffers.c module
- Explicitly excludes temporary buffers (isTemp = true) from being tracked
- Uses exponential growth strategy (doubling) for efficient array expansion
- Array expansion only occurs when current capacity is exceeded
- The loadedBuffersCount is incremented after successful addition to maintain accurate count
- This tracking enables efficient memory management by identifying which buffers can be swapped to disk
- Array initially starts with capacity of 32 buffers (set in gistInitBuildBuffers)