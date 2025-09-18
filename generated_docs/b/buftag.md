# buftag

## Location
[src/include/storage/buf_internals.h:93-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L93-L99)

## Overview
The  is a fundamental data structure that uniquely identifies which disk block a buffer contains in PostgreSQL's buffer management system.

## Definition


## Detailed Description
The  structure (typedef'd as ) serves as a complete identifier for any disk block in PostgreSQL's storage system. It contains all the necessary information to uniquely identify and locate a specific block without requiring access to system catalogs like pg_class or pg_tablespace. This design is crucial for buffer management operations that may occur in contexts where the relation might not yet be visible to the current transaction.

The structure is designed to be used as a hash key, which requires careful handling of any potential padding bytes to ensure consistent hashing behavior across the system.

## Parameters / Member Variables
- : The OID of the tablespace containing the relation
- : The OID of the database containing the relation  
- : The file number of the relation within the database
- : The fork number indicating which fork of the relation (main, FSM, VM, etc.)
- : The block number relative to the beginning of the relation

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileNumber](../R/RelFileNumber.md) (type)
  - [ForkNumber](../F/ForkNumber.md) (type)
  - BlockNumber (type) 
  - Oid (type)
- Called from (representative examples):
  - [BufTableHashCode](../B/BufTableHashCode.md) (for hash table operations)
  - [BufTableLookup](../B/BufTableLookup.md) (for buffer lookups)
  - [BufferAlloc](../B/BufferAlloc.md) (for buffer allocation)
  - SyncOneBuffer (for buffer synchronization)
  - [InitBufferTag](../I/InitBufferTag.md) (for tag initialization)
  - [BufferTagsEqual](../B/BufferTagsEqual.md) (for tag comparison)

## Notes and Other Information
- The structure must contain no padding bytes when used as a hash key, requiring careful initialization via InitBufferTag
- The design ensures that buffer flushing operations can proceed without dependency on catalog visibility
- This is a core component of PostgreSQL's shared buffer pool management system
- The tag provides complete location information independent of transaction visibility rules