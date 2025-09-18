# BufTagGetRelNumber

## Location
[src/include/storage/buf_internals.h:103-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L103-L108)

## Overview
Extracts the relation number (RelFileNumber) from a BufferTag structure for buffer management operations.

## Definition


## Detailed Description
BufTagGetRelNumber is a simple inline accessor function that retrieves the relation number component from a BufferTag structure. The relation number is a key identifier used in PostgreSQL's buffer management system to identify specific relations (tables, indexes, etc.) within the buffer pool. This function provides a clean interface for accessing the relNumber field of the BufferTag structure while maintaining encapsulation.

## Parameters / Member Variables
- : Pointer to a const BufferTag structure from which to extract the relation number

## Dependencies
- Functions called/Symbols referenced:
  - BufferTag (structure type)
- Called from (representative examples):
  - BufferSync
  - [BufTagGetRelFileLocator](BufTagGetRelFileLocator.md)
  - [BufTagMatchesRelFileLocator](BufTagMatchesRelFileLocator.md)

## Notes and Other Information
- This is an inline function defined in buf_internals.h for performance efficiency
- The function is read-only (const parameter) and has no side effects
- Part of the buffer tag utility functions that provide abstracted access to BufferTag fields
- Used primarily in buffer management and synchronization operations