# SMgrRelationData

## Location
[src/include/storage/smgr.h:34-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/smgr.h#L34-L69)

## Overview
A structure that represents a cached file handle in PostgreSQL's storage manager, containing metadata and state information for managing I/O operations on relation files.

## Definition
```c
typedef struct SMgrRelationData
{
    /* rlocator is the hashtable lookup key, so it must be first! */
    RelFileLocatorBackend smgr_rlocator;        /* relation physical identifier */

    /*
     * The following fields are reset to InvalidBlockNumber upon a cache flush
     * event, and hold the last known size for each fork.  This information is
     * currently only reliable during recovery, since there is no cache
     * invalidation for fork extension.
     */
    BlockNumber smgr_targblock;                 /* current insertion target block */
    BlockNumber smgr_cached_nblocks[MAX_FORKNUM + 1];  /* last known size */

    /* additional public fields may someday exist here */

    /*
     * Fields below here are intended to be private to smgr.c and its
     * submodules.  Do not touch them from elsewhere.
     */
    int         smgr_which;                     /* storage manager selector */

    /*
     * for md.c; per-fork arrays of the number of open segments
     * (md_num_open_segs) and the segments themselves (md_seg_fds).
     */
    int         md_num_open_segs[MAX_FORKNUM + 1];
    struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];

    /*
     * Pinning support.  If unpinned (ie. pincount == 0), 'node' is a list
     * link in list of all unpinned SMgrRelations.
     */
    int         pincount;
    dlist_node  node;
} SMgrRelationData;
```

## Detailed Description
`SMgrRelationData` is the core data structure of PostgreSQL's storage manager system, representing a cached file handle for database relations. It serves as an abstraction layer that maintains metadata about physical files on disk while providing a unified interface for different storage implementations. The structure is designed to be efficient for hashtable lookups (with `smgr_rlocator` as the first field) and supports reference counting through pinning to prevent premature destruction while in use.

The structure maintains both public and private fields, with clear separation between fields intended for general use and those reserved for internal storage manager operations. It supports multiple relation forks (main data, free space map, visibility map, etc.) through per-fork arrays.

## Parameters / Member Variables
- `smgr_rlocator`: RelFileLocatorBackend serving as the hashtable lookup key and physical identifier for the relation
- `smgr_targblock`: BlockNumber indicating the current insertion target block for new data
- `smgr_cached_nblocks[]`: Array storing the last known size for each fork (reliable primarily during recovery)
- `smgr_which`: Integer selector identifying which storage manager implementation to use
- `md_num_open_segs[]`: Array tracking the number of open segments for each fork (used by md.c)
- `md_seg_fds[]`: Array of pointers to segment file descriptors for each fork (used by md.c)
- `pincount`: Integer reference count for preventing destruction while the relation is in use
- `node`: dlist_node for linking unpinned SMgrRelations in a doubly-linked list

## Dependencies
- Functions called/Symbols referenced:
  - `RelFileLocatorBackend`
  - `MAX_FORKNUM`
  - `_MdfdVec`
  - `dlist_node`
- Called from (representative examples):
  - `smgropen`
  - `smgrdestroyall`
  - `BufferManagerRelation`
  - `ReadBuffersOperation`
  - `BulkWriteBuffer`

## Notes and Other Information
- The structure is designed as essentially cached file handles managed by smgr.c
- Creation (`smgropen`) and destruction (`smgrdestroy`) operations do not imply I/O, only hashtable management
- SMgrRelations can be "pinned" to prevent destruction while in use, particularly for relcache pointers
- Unpinned relations are automatically deleted at end of transaction
- Cache flush events reset the cached block numbers to InvalidBlockNumber
- The md.c-specific fields support the magnetic disk storage manager's segment-based file organization
- `SMgrRelation` is defined as `typedef SMgrRelationData *SMgrRelation` for convenient pointer usage