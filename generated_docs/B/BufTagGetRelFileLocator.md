# BufTagGetRelFileLocator

## Location
src/include/storage/buf_internals.h: 123 - 134

## Overview
Constructs and returns a RelFileLocator structure from the components stored in a BufferTag.

## Definition
static inline RelFileLocator
BufTagGetRelFileLocator(const BufferTag *tag)

## Detailed Description
BufTagGetRelFileLocator is an inline utility function that extracts the file location information from a BufferTag and constructs a complete RelFileLocator structure. The RelFileLocator contains the tablespace OID, database OID, and relation number that together uniquely identify a relation file in the PostgreSQL storage system. This function provides a convenient way to obtain the full file locator information from a buffer tag, which is frequently needed for file operations and relation identification.

## Parameters / Member Variables
- tag: Pointer to a const BufferTag structure from which to extract the file location information

## Dependencies
- Functions called/Symbols referenced:
  - BufferTag (structure type)
  - BufTagGetRelNumber (function)
  - RelFileLocator (structure type)
- Called from (representative examples):
  - DebugPrintBufferRefcount
  - BufferGetTag
  - FlushBuffer
  - buffertag_comparator
  - IssuePendingWritebacks

## Notes and Other Information
- This is an inline function defined in buf_internals.h for performance efficiency
- Constructs a RelFileLocator by copying spcOid and dbOid directly from the tag and using BufTagGetRelNumber for the relation number
- The returned RelFileLocator can be used for file system operations and relation identification
- Widely used throughout buffer management for operations that need to identify the underlying relation files
- Does not include fork or block number information - only the relation file location