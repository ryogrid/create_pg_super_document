# ReadBufferWithoutRelcache

## Location
[src/backend/storage/buffer/bufmgr.c:829-844](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L829-L844)

## Overview
Reads a buffer block from a relation without requiring a relcache entry, providing a lightweight alternative to ReadBufferExtended for scenarios where relation metadata is not available or needed.

## Definition
```c
Buffer ReadBufferWithoutRelcache(RelFileLocator rlocator, ForkNumber forkNum,
                                 BlockNumber blockNum, ReadBufferMode mode,
                                 BufferAccessStrategy strategy, bool permanent)
```

## Detailed Description
ReadBufferWithoutRelcache serves as a specialized buffer reading function that bypasses the relation cache (relcache) system. This function is particularly useful in recovery scenarios, system maintenance operations, or when accessing relations where the relcache entry might not be available or reliable.

The function opens a storage manager relation directly using the provided RelFileLocator and delegates the actual buffer reading to ReadBuffer_common. It determines the relation persistence type based on the permanent parameter, mapping it to either RELPERSISTENCE_PERMANENT or RELPERSISTENCE_UNLOGGED.

Note that this function cannot handle temporary relations, as temporary relations have process-specific characteristics that would require additional handling.

## Parameters / Member Variables
- `rlocator`: RelFileLocator specifying the physical location of the relation file
- `forkNum`: Fork number indicating which fork of the relation to read from (main, FSM, VM, etc.)
- `blockNum`: Block number within the specified fork to read
- `mode`: ReadBufferMode specifying the reading behavior (normal, zero on error, etc.)
- `strategy`: BufferAccessStrategy for controlling buffer replacement policy, can be NULL for default behavior
- `permanent`: Boolean flag indicating relation persistence (true for permanent, false for unlogged relations)

## Dependencies
- Functions called/Symbols referenced:
  - [smgropen](../s/smgropen.md)
  - [ReadBuffer_common](ReadBuffer_common.md)
  - INVALID_PROC_NUMBER
  - RELPERSISTENCE_PERMANENT
  - RELPERSISTENCE_UNLOGGED
- Called from (representative examples):
  - [XLogReadBufferExtended](../X/XLogReadBufferExtended.md)
  - [ScanSourceDatabasePgClass](../S/ScanSourceDatabasePgClass.md)
  - [RelationCopyStorageUsingBuffer](RelationCopyStorageUsingBuffer.md)

## Notes and Other Information
- Cannot be used for temporary relations due to their process-specific nature
- Provides a more direct path to buffer access when relcache entries are unavailable
- Commonly used in WAL recovery and database maintenance operations
- The permanent parameter is critical for proper buffer management and determines the persistence characteristics used internally