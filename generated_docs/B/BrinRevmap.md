# BrinRevmap

## Location
src/backend/access/brin/brin_revmap.c: 46 - 69

## Overview
BrinRevmap is a data structure that manages access to the BRIN (Block Range INdex) reverse mapping, providing an interface to locate and manage the mapping between heap blocks and their corresponding index entries.

## Definition
```c
struct BrinRevmap
{
    Relation    rm_irel;
    BlockNumber rm_pagesPerRange;
    BlockNumber rm_lastRevmapPage;    /* cached from the metapage */
    Buffer      rm_metaBuf;
    Buffer      rm_currBuf;
};
```

## Detailed Description
The BrinRevmap structure serves as an access object for the BRIN reverse mapping functionality. It maintains state information needed to efficiently navigate and manipulate the reverse mapping data structure that maps heap block ranges to their corresponding BRIN index entries. The structure caches critical metadata and maintains buffer references to avoid repeated I/O operations during reverse mapping operations.

The reverse mapping is essential for BRIN index operations as it allows the system to quickly locate which index page contains summary information for a given range of heap blocks. This structure encapsulates all the necessary state for performing such lookups and updates efficiently.

## Parameters / Member Variables
- `rm_irel`: The BRIN index relation that this reverse mapping belongs to
- `rm_pagesPerRange`: The number of heap pages that each BRIN index entry summarizes (cached from metadata)
- `rm_lastRevmapPage`: The last page number in the reverse mapping, cached from the metapage for performance
- `rm_metaBuf`: Buffer containing the BRIN metapage, kept pinned for efficient access to metadata
- `rm_currBuf`: Buffer for the currently accessed reverse mapping page, may be InvalidBuffer when not in use

## Dependencies
- Functions called/Symbols referenced:
  - [revmap_get_blkno](../r/revmap_get_blkno.md)
  - [revmap_get_buffer](../r/revmap_get_buffer.md)  
  - [revmap_extend_and_get_blkno](../r/revmap_extend_and_get_blkno.md)
  - [revmap_physical_extend](../r/revmap_physical_extend.md)
- Called from (representative examples):
  - [brinRevmapInitialize](../b/brinRevmapInitialize.md)
  - [brinRevmapTerminate](../b/brinRevmapTerminate.md)
  - [brinRevmapExtend](../b/brinRevmapExtend.md)
  - [brinLockRevmapPageForUpdate](../b/brinLockRevmapPageForUpdate.md)
  - [brinGetTupleForHeapBlock](../b/brinGetTupleForHeapBlock.md)
  - [brinRevmapDesummarizeRange](../b/brinRevmapDesummarizeRange.md)

## Notes and Other Information
- The BrinRevmap structure is allocated and initialized via brinRevmapInitialize() and must be freed using brinRevmapTerminate()
- The structure maintains buffer pins to avoid repeated I/O, but these must be properly released during cleanup
- The rm_currBuf member is set to InvalidBuffer when no reverse mapping page is currently being accessed
- This structure is used extensively throughout BRIN index operations including insertion, building, summarization, and desummarization
- The reverse mapping mechanism is critical for BRIN index performance as it provides O(1) lookup of index pages for given heap block ranges