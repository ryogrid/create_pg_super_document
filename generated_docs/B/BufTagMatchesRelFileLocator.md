# BufTagMatchesRelFileLocator

## Location
[src/include/storage/buf_internals.h:164-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L164-L179)

## Overview
BufTagMatchesRelFileLocator is an inline function that determines whether a BufferTag corresponds to the same relation as specified by a RelFileLocator by comparing their tablespace, database, and relation identifiers.

## Definition
static inline bool
BufTagMatchesRelFileLocator(const BufferTag *tag,
                           const RelFileLocator *rlocator)

## Detailed Description
BufTagMatchesRelFileLocator provides a targeted comparison between a BufferTag and a RelFileLocator to determine if they refer to the same database relation. Unlike BufferTagsEqual which requires exact block and fork matches, this function only compares the relation-level identifiers: tablespace OID, database OID, and relation number.

This function is particularly useful in buffer management operations that need to work at the relation level rather than the individual block level, such as dropping all buffers for a relation, flushing relation buffers, or performing relation-wide buffer operations. The function uses the BufTagGetRelNumber helper to extract the relation number from the BufferTag, ensuring consistent access to this field.

The comparison is essential for buffer management operations that target entire relations or need to identify all buffers belonging to a specific relation across different forks and blocks.

## Parameters / Member Variables
- : Pointer to the BufferTag structure to be checked
- : Pointer to the RelFileLocator structure containing the relation identifiers to match against

## Dependencies
- Functions called/Symbols referenced:
  - [BufTagGetRelNumber](BufTagGetRelNumber.md)
  - BufferTag (structure type)
  - [RelFileLocator](../R/RelFileLocator.md) (structure type)
- Called from (representative examples):
  - [ReleaseAndReadBuffer](../R/ReleaseAndReadBuffer.md)
  - [DropRelationBuffers](../D/DropRelationBuffers.md)
  - [DropRelationsAllBuffers](../D/DropRelationsAllBuffers.md)
  - [FindAndDropRelationBuffers](../F/FindAndDropRelationBuffers.md)
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md)
  - [FlushRelationsAllBuffers](../F/FlushRelationsAllBuffers.md)
  - [DropRelationLocalBuffers](../D/DropRelationLocalBuffers.md)
  - [DropRelationAllLocalBuffers](../D/DropRelationAllLocalBuffers.md)

## Notes and Other Information
- This is an inline function optimized for frequent relation-level buffer operations
- Compares only relation-level identifiers (spcOid, dbOid, relNumber), not block-level details
- Critical for relation-wide buffer management operations like dropping and flushing
- Used extensively in buffer cleanup operations during relation drops and truncations
- More efficient than full BufferTag comparison when only relation identity matters
- Essential for maintaining buffer consistency during DDL operations on relations