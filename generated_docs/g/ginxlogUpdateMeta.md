# ginxlogUpdateMeta

## Location
src/include/access/ginxlog.h: 168 - 178

## Overview
Structure used for WAL (Write-Ahead Logging) record when updating the metapage of a GIN index, typically during fast insertion operations.

## Definition
```c
typedef struct ginxlogUpdateMeta
{
    RelFileLocator locator;
    GinMetaPageData metadata;
    BlockNumber prevTail;
    BlockNumber newRightlink;
    int32       ntuples;        /* if ntuples > 0 then metadata.tail was
                                 * updated with that many tuples; else new sub
                                 * list was inserted */
    /* array of inserted tuples follows */
} ginxlogUpdateMeta;
```

## Detailed Description
The ginxlogUpdateMeta structure is used as part of WAL logging when updating the metapage of GIN (Generalized Inverted Index) indexes. This operation is fundamental to GIN's fast insertion mechanism, where new entries are initially stored in a pending list before being moved to the main index structure. The structure contains the complete metadata information needed to restore the metapage state, along with information about pending list updates and any tuples that were inserted.

## Parameters / Member Variables
- `locator`: File locator identifying the relation being modified
- `metadata`: Complete GinMetaPageData structure containing:
  - Pending list head/tail pointers
  - Free space tracking information
  - Statistics for the planner
  - Version information
- `prevTail`: Previous tail block number before the update
- `newRightlink`: New right link value for linking pages
- `ntuples`: Number of tuples being processed; if > 0, metadata.tail was updated with that many tuples; if <= 0, a new sublist was inserted

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocator
  - GinMetaPageData
  - BlockNumber
- Called from (representative examples):
  - ginHeapTupleFastInsert (in src/backend/access/gin/ginfast.c:227,434)
  - ginUpdateStats (in src/backend/access/gin/ginutil.c:683,691)
  - ginRedoUpdateMetapage (in src/backend/access/gin/ginxlog.c:531)

## Notes and Other Information
- This structure is part of the GIN index WAL logging infrastructure (XLOG_GIN_UPDATE_META_PAGE operation)
- Used primarily during fast insertion operations that update the pending list
- The structure is followed by an array of inserted tuples in the WAL record
- Backup includes both metapage (Blk 0) and tail page (Blk 1) as noted in comments
- During recovery, ginRedoUpdateMetapage restores the metapage from this record
- Critical for maintaining consistency of GIN's fast insertion mechanism
- Defined in src/include/access/ginxlog.h:164-178