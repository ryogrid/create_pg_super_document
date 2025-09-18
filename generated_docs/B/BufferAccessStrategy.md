# BufferAccessStrategy

## Location
[src/include/storage/buf.h:44-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf.h#L44-L46)

## Overview
BufferAccessStrategy is a pointer type that represents a strategy object for managing buffer access patterns in PostgreSQL, designed to optimize buffer pool usage for different types of operations like bulk reads, writes, and vacuum operations.

## Definition


## Detailed Description
BufferAccessStrategy is an opaque pointer type that encapsulates buffer management strategies in PostgreSQL's buffer pool system. The actual implementation is hidden in the BufferAccessStrategyData structure, which is private to . This design provides different buffer access patterns optimized for various database operations.

The strategy system implements a ring buffer approach where each strategy type uses a different sized ring of buffers to minimize interference with the normal buffer pool operations. This prevents large operations from evicting frequently-used pages from the shared buffer pool.

The system supports four main strategy types:
- **BAS_NORMAL**: Normal random access (returns NULL, uses default buffer management)
- **BAS_BULKREAD**: Large read-only scans with 256KB ring size
- **BAS_BULKWRITE**: Large multi-block writes (e.g., COPY IN) with 16MB ring size  
- **BAS_VACUUM**: VACUUM operations with 2MB ring size

## Parameters / Member Variables
The underlying BufferAccessStrategyData structure contains:
- : The overall strategy type (BufferAccessStrategyType enum)
- : Number of elements in the buffers array
- : Index of the current slot in the ring (most recently returned)
- : Flexible array of buffer numbers, with InvalidBuffer indicating unselected slots

## Dependencies
- Functions called/Symbols referenced:
  - BufferAccessStrategyData (underlying struct)
  - BufferAccessStrategyType (enum for strategy types)
  - Buffer (buffer identifier type)

- Called from (representative examples):
  - GetAccessStrategy
  - GetAccessStrategyWithSize
  - FreeAccessStrategy
  - StrategyGetBuffer
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [vacuum_rel](../v/vacuum_rel.md)
  - [analyze_rel](../a/analyze_rel.md)
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md)

## Notes and Other Information
- The strategy object is allocated in the current memory context when created
- BAS_NORMAL strategy returns NULL as it uses the default buffer management
- Ring sizes are carefully chosen based on empirical testing and documented rationales in buffer/README
- The system helps prevent large operations from polluting the shared buffer cache
- Used extensively in vacuum, analyze, bulk operations, and various access methods
- The design allows for future extension with additional strategy types
- IO statistics are tracked separately for each strategy type through corresponding IOContext values