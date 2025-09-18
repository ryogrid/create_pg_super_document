# BufTagGetForkNum

## Location
src/include/storage/buf_internals.h: 109 - 114

## Overview
Extracts the fork number (ForkNumber) from a BufferTag structure to identify which fork of a relation the buffer contains.

## Definition
static inline ForkNumber
BufTagGetForkNum(const BufferTag *tag)

## Detailed Description
BufTagGetForkNum is an inline accessor function that retrieves the fork number component from a BufferTag structure. In PostgreSQL, relations can have multiple forks (main data, free space map, visibility map, etc.), and the fork number identifies which specific fork a buffer belongs to. This function provides encapsulated access to the forkNum field of the BufferTag structure, which is essential for buffer management operations that need to distinguish between different forks of the same relation.

## Parameters / Member Variables
- tag: Pointer to a const BufferTag structure from which to extract the fork number

## Dependencies
- Functions called/Symbols referenced:
  - BufferTag (structure type)
- Called from (representative examples):
  - ReleaseAndReadBuffer
  - BufferSync
  - FlushBuffer
  - DropRelationBuffers
  - buffertag_comparator

## Notes and Other Information
- This is an inline function defined in buf_internals.h for performance efficiency
- The function is read-only (const parameter) and has no side effects
- Fork numbers distinguish between main data fork, free space map fork, visibility map fork, etc.
- Extensively used throughout the buffer management system for fork-specific operations
- Critical for proper buffer identification and management across different relation forks