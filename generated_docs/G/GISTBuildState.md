# GISTBuildState

## Location
[src/backend/access/gist/gistbuild.c:111-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L111-L112)

## Overview
A structure that maintains the working state and context information during GiST index construction, supporting multiple build strategies including buffering and sorting modes.

## Definition
```c
typedef struct
{
    Relation    indexrel;
    Relation    heaprel;
    GISTSTATE  *giststate;

    Size        freespace;      /* amount of free space to leave on pages */

    GistBuildMode buildMode;

    int64       indtuples;      /* number of tuples indexed */

    /*
     * Extra data structures used during a buffering build. 'gfbb' contains
     * information related to managing the build buffers. 'parentMap' is a
     * lookup table of the parent of each internal page.
     */
    int64       indtuplesSize;  /* total size of all indexed tuples */
    GISTBuildBuffers *gfbb;
    HTAB       *parentMap;

    /*
     * Extra data structures used during a sorting build.
     */
    Tuplesortstate *sortstate;  /* state data for tuplesort.c */

    BlockNumber pages_allocated;

    BulkWriteState *bulkstate;
} GISTBuildState;
```

## Detailed Description
GISTBuildState serves as the central context structure for GiST index building operations. It maintains references to the index and heap relations, tracks build progress, and contains mode-specific data structures for both buffering and sorting build strategies. The structure is passed between various callback functions during index construction to maintain state consistency.

## Parameters / Member Variables
- `indexrel`: The index relation being built
- `heaprel`: The heap relation being indexed
- `giststate`: GiST-specific state information and operator functions
- `freespace`: Amount of free space to reserve on each page for future insertions
- `buildMode`: Current build strategy from the GistBuildMode enumeration
- `indtuples`: Count of tuples that have been indexed so far
- `indtuplesSize`: Total size in bytes of all indexed tuples (used for buffering mode decisions)
- `gfbb`: Build buffer management structure used during buffering builds
- `parentMap`: Hash table mapping internal pages to their parent pages (buffering mode)
- `sortstate`: Tuplesort state for managing sorted builds
- `pages_allocated`: Number of pages allocated for the index
- `bulkstate`: Bulk write state for efficient page writing

## Dependencies
- Functions called/Symbols referenced:
  - [GISTSTATE](GISTSTATE.md)
  - [GistBuildMode](GistBuildMode.md)
  - [GISTBuildBuffers](GISTBuildBuffers.md)
  - [HTAB](../H/HTAB.md)
  - [Tuplesortstate](../T/Tuplesortstate.md)
  - [BulkWriteState](../B/BulkWriteState.md)
- Called from (representative examples):
  - [gistbuild](../g/gistbuild.md)
  - [gistBuildCallback](../g/gistBuildCallback.md)
  - [gistSortedBuildCallback](../g/gistSortedBuildCallback.md)
  - [gist_indexsortbuild](../g/gist_indexsortbuild.md)
  - [gistInitBuffering](../g/gistInitBuffering.md)
  - [gistBufferingBuildInsert](../g/gistBufferingBuildInsert.md)

## Notes and Other Information
The structure is designed to accommodate different build strategies efficiently. The buffering-specific fields (gfbb, parentMap, indtuplesSize) are only used when buildMode indicates buffering, while sortstate is only used for sorted builds. This design allows the same structure to support multiple build algorithms without unnecessary memory overhead.