# GinMetaPageData

## Location
[src/include/access/ginblock.h:55-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/ginblock.h#L55-L101)

## Overview
GinMetaPageData is the metadata structure stored on GIN index metapages, containing comprehensive information about the index state, pending list management, and statistics for query planning.

## Definition
```c
typedef struct GinMetaPageData
{
    /* Pointers to head and tail of pending list */
    BlockNumber head;
    BlockNumber tail;
    
    /* Free space in bytes in the pending list's tail page */
    uint32      tailFreeSize;
    
    /* Pending list statistics */
    BlockNumber nPendingPages;
    int64       nPendingHeapTuples;
    
    /* Statistics for planner use (accurate as of last VACUUM) */
    BlockNumber nTotalPages;
    BlockNumber nEntryPages;
    BlockNumber nDataPages;
    int64       nEntries;
    
    /* GIN version number */
    int32       ginVersion;
} GinMetaPageData;
```

## Detailed Description
GinMetaPageData serves as the central metadata repository for GIN indexes, stored on the metapage (typically block 0). This structure manages two critical aspects of GIN indexes: the pending list mechanism for fast insertions and comprehensive statistics for query optimization.

The pending list is a key performance feature of GIN indexes, allowing fast insertions by temporarily storing new entries in a separate list structure before they are merged into the main index during cleanup operations. The metadata tracks both the head and tail of this list, along with space utilization information.

The statistics portion provides the PostgreSQL query planner with accurate information about index size and composition, including total pages, entry pages, data pages, and the total number of entries. These statistics are updated during VACUUM operations and are crucial for optimal query planning.

The ginVersion field tracks the index format version, enabling backward compatibility and format migration when the index structure evolves across PostgreSQL versions.

## Parameters / Member Variables
- `head`: BlockNumber pointing to the first page of the pending list chain
- `tail`: BlockNumber pointing to the last page of the pending list chain  
- `tailFreeSize`: uint32 indicating free space in bytes available in the pending list's tail page
- `nPendingPages`: BlockNumber counting total pages in the pending list
- `nPendingHeapTuples`: int64 counting total heap tuples stored in the pending list
- `nTotalPages`: BlockNumber indicating total pages in the entire GIN index (accurate as of last VACUUM)
- `nEntryPages`: BlockNumber counting entry tree pages (accurate as of last VACUUM)
- `nDataPages`: BlockNumber counting posting tree/list data pages (accurate as of last VACUUM)
- `nEntries`: int64 indicating total number of index entries (accurate as of last VACUUM)
- `ginVersion`: int32 storing the GIN index format version number (currently 2 for indexes created in 9.4+)

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (PostgreSQL block numbering type)
  - int64 (PostgreSQL 64-bit integer type)
  - uint32 (32-bit unsigned integer type)
  - int32 (32-bit signed integer type)
- Called from (representative examples):
  - GinPageGetMeta (macro for accessing metapage data)
  - [ginHeapTupleFastInsert](../g/ginHeapTupleFastInsert.md) (fast insertion operations)
  - [ginInsertCleanup](../g/ginInsertCleanup.md) (pending list cleanup)
  - [GinInitMetabuffer](GinInitMetabuffer.md) (metapage initialization)
  - [ginGetStats](../g/ginGetStats.md) (statistics retrieval)
  - [ginUpdateStats](../g/ginUpdateStats.md) (statistics updates)
  - [ginRedoUpdateMetapage](../g/ginRedoUpdateMetapage.md) (WAL replay)
  - [makeSublist](../m/makeSublist.md) (pending list management)
  - [shiftList](../s/shiftList.md) (pending list reorganization)

## Notes and Other Information
- Version compatibility: Version 2 (9.4+) is current, Version 1 (9.1-9.3) may contain uncompressed pages, Version 0 (9.0-) may miss null entries
- The ginVersion field was placed at the end for compatibility reasons and should not be moved
- Statistics fields are only accurate as of the last VACUUM operation
- The pending list mechanism is crucial for GIN performance, allowing batch processing of insertions
- Full index scans are rejected on Version 0 indexes due to potential missing null entries
- Located in src/include/access/ginblock.h at lines 55-101