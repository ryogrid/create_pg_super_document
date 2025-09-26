# ClearBufferTag

## Location
[src/include/storage/buf_internals.h:135-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L135-L143)

## Overview
Initializes a BufferTag structure by setting all its fields to invalid values, effectively clearing the tag.

## Definition
static inline void
ClearBufferTag(BufferTag *tag)

## Detailed Description
ClearBufferTag is an inline utility function that initializes a BufferTag structure to an invalid/cleared state by setting all its component fields to their respective invalid values. This function is used during buffer initialization, buffer invalidation, and when preparing buffer tags for reuse. It ensures that the BufferTag is in a clean, well-defined state with no residual data from previous usage.

## Parameters / Member Variables
- tag: Pointer to a BufferTag structure to be cleared/invalidated

## Dependencies
- Functions called/Symbols referenced:
  - BufferTag (structure type)
  - InvalidOid (constant)
  - InvalidRelFileNumber (constant)
  - InvalidForkNumber (constant)
  - InvalidBlockNumber (constant)
  - [BufTagSetRelForkDetails](../B/BufTagSetRelForkDetails.md) (function)
- Called from (representative examples):
  - [InitBufferPool](../I/InitBufferPool.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [InvalidateVictimBuffer](../I/InvalidateVictimBuffer.md)
  - [GetLocalVictimBuffer](../G/GetLocalVictimBuffer.md)
  - [DropRelationLocalBuffers](../D/DropRelationLocalBuffers.md)

## Notes and Other Information
- This is an inline function defined in buf_internals.h for performance efficiency
- Sets spcOid and dbOid to InvalidOid
- Uses BufTagSetRelForkDetails to set relation number and fork number to invalid values
- Sets blockNum to InvalidBlockNumber
- Essential for proper buffer management lifecycle and ensuring clean buffer states
- Used extensively during buffer pool initialization and buffer invalidation operations