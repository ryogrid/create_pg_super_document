# InitBufferTag

## Location
[src/include/storage/buf_internals.h:144-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L144-L153)

## Overview
InitBufferTag is an inline function that initializes a BufferTag structure with the essential information needed to uniquely identify a specific block within PostgreSQL's buffer management system.

## Definition


## Detailed Description
InitBufferTag serves as a convenience function for properly initializing a BufferTag structure. The BufferTag is a critical data structure in PostgreSQL's buffer management that uniquely identifies a specific database block across the entire system. This function takes a RelFileLocator (which contains tablespace, database, and relation identifiers) along with fork and block numbers, and populates all the necessary fields of the BufferTag structure.

The function extracts the tablespace OID and database OID directly from the RelFileLocator, then delegates the setting of relation number and fork number to the BufTagSetRelForkDetails helper function, and finally sets the block number. This initialization is essential for buffer lookup operations, as the BufferTag serves as the key for finding buffers in the buffer hash table.

## Parameters / Member Variables
- : Pointer to the BufferTag structure to be initialized
- : Pointer to RelFileLocator containing tablespace, database, and relation identifiers
- : Fork number identifying which fork of the relation (main, FSM, VM, etc.)
- : Block number within the specified fork of the relation

## Dependencies
- Functions called/Symbols referenced:
  - [BufTagSetRelForkDetails](../B/BufTagSetRelForkDetails.md)
  - BufferTag (structure type)
- Called from (representative examples):
  - [PrefetchSharedBuffer](../P/PrefetchSharedBuffer.md)
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md)
  - [BufferAlloc](../B/BufferAlloc.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)
  - [FindAndDropRelationBuffers](../F/FindAndDropRelationBuffers.md)
  - PrefetchLocalBuffer
  - LocalBufferAlloc
  - ExtendBufferedRelLocal

## Notes and Other Information
- This is an inline function defined in buf_internals.h for performance optimization
- The BufferTag structure contains: spcOid, dbOid, relNumber, forkNum, and blockNum
- Used extensively throughout the buffer management subsystem for buffer identification
- Critical for proper buffer hash table operations and buffer lookup functionality
- The function ensures consistent initialization of BufferTag structures across the codebase