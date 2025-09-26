# BlockRefTableEntry

## Location
[src/common/blkreftable.c:110-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L110-L121)

## Overview
BlockRefTableEntry represents the state and tracking information for one specific relation fork within PostgreSQL's block reference table system, managing block modification status using a chunked storage approach.

## Definition
```c
struct BlockRefTableEntry
{
    BlockRefTableKey key;
    BlockNumber limit_block;
    char        status;
    uint32      nchunks;
    uint16     *chunk_size;
    uint16     *chunk_usage;
    BlockRefTableChunk *chunk_data;
};
```

## Detailed Description
BlockRefTableEntry maintains comprehensive state information for tracking block modifications within a specific relation fork. The structure uses a sophisticated chunked storage system to efficiently represent the modification status of blocks. Each chunk can operate in two modes: as an array when few blocks are modified, or as a bitmap when many blocks are modified. This adaptive approach optimizes both memory usage and access performance. The entry tracks the known length of the relation and manages dynamic allocation of chunks as needed to cover the full range of blocks in the relation.

## Parameters / Member Variables
- `key`: BlockRefTableKey that uniquely identifies the relation fork this entry represents
- `limit_block`: The shortest known length of the relation in blocks within the LSN range; set to 0 for created/dropped relations, or truncated length for truncated relations  
- `status`: Status flag indicating the overall state of this relation fork entry
- `nchunks`: The allocated length of the chunk arrays; determines the maximum representable block number (nchunks * BLOCKS_PER_CHUNK)
- `chunk_size`: Array storing the allocated size of each chunk for dynamic memory management
- `chunk_usage`: Array tracking the number of elements used in each chunk; determines whether chunk operates as array or bitmap
- `chunk_data`: Array of BlockRefTableChunk structures containing the actual block modification data

## Dependencies
- Functions called/Symbols referenced:
  - [BlockRefTableKey](BlockRefTableKey.md) (as the key member)
  - BlockRefTableChunk (for chunk_data array)
- Used by:
  - [GetFileBackupMethod](../G/GetFileBackupMethod.md) (incremental backup functionality)
  - SH_ELEMENT_TYPE (hash table element type)
  - [BlockRefTableSetLimitBlock](BlockRefTableSetLimitBlock.md)
  - [BlockRefTableMarkBlockModified](BlockRefTableMarkBlockModified.md)
  - [BlockRefTableGetEntry](BlockRefTableGetEntry.md)
  - [BlockRefTableEntryGetBlocks](BlockRefTableEntryGetBlocks.md)
  - [WriteBlockRefTable](../W/WriteBlockRefTable.md)
  - [BlockRefTableWriteEntry](BlockRefTableWriteEntry.md)
  - [CreateBlockRefTableEntry](../C/CreateBlockRefTableEntry.md)
  - [BlockRefTableEntrySetLimitBlock](BlockRefTableEntrySetLimitBlock.md)
  - [BlockRefTableEntryMarkBlockModified](BlockRefTableEntryMarkBlockModified.md)
  - [BlockRefTableFreeEntry](BlockRefTableFreeEntry.md)
  - Various header function declarations

## Notes and Other Information
- Defined in src/common/blkreftable.c:110-121 with extensive documentation at lines 84-119
- Uses adaptive chunk storage: arrays for sparse modifications, bitmaps for dense modifications
- The chunk_usage value determines storage mode: less than MAX_ENTRIES_PER_CHUNK means array mode, else bitmap mode
- In bitmap mode, the least significant bit of the first array element represents the lowest-numbered block in that chunk
- Critical component for PostgreSQL's incremental backup functionality
- Supports efficient tracking of block modifications across large relations through chunked representation