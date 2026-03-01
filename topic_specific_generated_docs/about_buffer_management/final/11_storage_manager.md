# Storage Manager Layer

[<< WAL Integration](10_wal_integration.md) | [Index](index.md) | [Next: Data Movement and Durability >>](12_data_movement_and_durability.md)

---

## Overview

The storage manager (smgr) provides a pluggable abstraction layer between the buffer manager and the physical file system. It mediates all I/O operations on relation data files, translating buffer-manager requests (read page, write page, extend relation) into file system operations. Although the API supports multiple storage backends via function pointer tables, PostgreSQL currently only implements one: the "magnetic disk" manager (`md.c`), which manages files using the Virtual File Descriptor (VFD) layer.

See diagram: [storage_stack.mermaid](../diagrams/storage_stack.mermaid)

## Architecture

```
Buffer Manager (bufmgr.c)
        |
        | smgrread(), smgrwrite(), smgrextend(), ...
        v
Storage Manager Interface (smgr.c)
        |
        | f_smgr function pointer dispatch
        v
MD Layer (md.c)
        |
        | FileRead(), FileWrite(), ...
        v
VFD Layer (fd.c)
        |
        | read(), write(), fsync(), ...
        v
Operating System / File System
```

## SMgrRelation

Source: `src/include/storage/smgr.h:34`

```c
typedef struct SMgrRelationData
{
    RelFileLocatorBackend smgr_rlocator;                   /* relation physical identifier */
    BlockNumber smgr_targblock;                            /* current insertion target block */
    BlockNumber smgr_cached_nblocks[MAX_FORKNUM + 1];     /* cached relation size per fork */
    int         smgr_which;                                /* storage manager selector (always 0 = md) */
    int         md_num_open_segs[MAX_FORKNUM + 1];
    struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];
    int         pincount;
    dlist_node  node;
} SMgrRelationData;
```

Each backend maintains a hash table of `SMgrRelation` objects, one per relation accessed. These are essentially cached file handles with the following lifecycle:

- **Created** by `smgropen()` on first access.
- **Pinned** by the relcache to prevent premature destruction.
- **Unpinned** entries are destroyed at end of transaction by `AtEOXact_SMgr()`.
- **Cached sizes**: `smgr_cached_nblocks[]` avoids repeated `lseek()` calls during recovery.

See [Data Structures Appendix](appendix_data_structures.md) for the full annotated definition.

## Core SMgr API

### smgropen()

Source: `src/backend/storage/smgr/smgr.c:197`

```c
SMgrRelation smgropen(RelFileLocator rlocator, ProcNumber backend)
```

Returns an `SMgrRelation` object for the given relation file locator, creating one if it does not exist. Note that `smgropen()` does NOT open any physical files -- actual file descriptors are obtained lazily on first I/O.

### smgrread() / smgrreadv()

Source: `src/include/storage/smgr.h`

```c
static inline void
smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
         void *buffer)
{
    smgrreadv(reln, forknum, blocknum, &buffer, 1);
}
```

`smgrread()` is an inline wrapper around `smgrreadv()`, which dispatches to `mdreadv()`. Called by [WaitReadBuffers()](05_buffer_access_protocol.md) to load pages from disk.

### smgrwrite() / smgrwritev()

```c
static inline void
smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
          const void *buffer, bool skipFsync)
```

Writes a page to the kernel page cache. Called by [FlushBuffer()](09_dirty_buffer_and_writeback.md). The `skipFsync` parameter controls whether the write is registered for deferred fsync. See [Data Movement and Durability](12_data_movement_and_durability.md).

### smgrextend()

```c
void smgrextend(SMgrRelation reln, ForkNumber forknum,
                BlockNumber blocknum, const void *buffer, bool skipFsync)
```

Extends a relation by writing a new block at the specified position.

### smgrnblocks()

```c
BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum)
```

Returns the number of blocks in the specified fork. May involve an `lseek()` call. The result is cached in `smgr_cached_nblocks[forknum]`.

### smgrwriteback()

```c
void smgrwriteback(SMgrRelation reln, ForkNumber forknum,
                   BlockNumber blocknum, BlockNumber nblocks)
```

Advises the kernel to write back dirty pages for the specified block range. See [Data Movement and Durability](12_data_movement_and_durability.md) for platform-specific details.

### smgrimmedsync()

```c
void smgrimmedsync(SMgrRelation reln, ForkNumber forknum)
```

Immediately fsyncs the relation fork. Used for operations that bypass WAL.

## MD Layer (Magnetic Disk Manager)

### Segment Layout

MD splits relation forks into segments of at most `RELSEG_SIZE` blocks (default: 131,072 blocks = 1 GB per segment). Segment files are named:

```
base/dboid/relfilenode        -- segment 0
base/dboid/relfilenode.1      -- segment 1
base/dboid/relfilenode.2      -- segment 2
...
```

### MdfdVec

```c
typedef struct _MdfdVec
{
    File    mdfd_vfd;       /* virtual file descriptor */
    BlockNumber mdfd_segno; /* segment number */
} MdfdVec;
```

Each open segment is tracked by an `MdfdVec` entry. Segments are opened lazily by `_mdfd_getseg()`.

### mdreadv()

Source: `src/backend/storage/smgr/md.c:806`

```c
void mdreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
             void **buffers, BlockNumber nblocks)
```

Reads one or more blocks from a relation fork:

1. Locates the correct segment via `_mdfd_getseg()`.
2. Computes offset: `seekpos = (off_t) BLCKSZ * (blocknum % RELSEG_SIZE)`.
3. Uses `FileReadV()` for vectorized I/O (`preadv()`).
4. Handles short reads with a retry loop.
5. On EOF, either zeros the page (if `zero_damaged_pages` or `InRecovery`) or raises ERROR.

### mdwritev()

Similar to `mdreadv()` but calls `FileWriteV()` for vectorized writes. If `skipFsync` is false, registers the file for fsync via `register_dirty_segment()`.

### mdwriteback()

```c
void mdwriteback(SMgrRelation reln, ForkNumber forknum,
                 BlockNumber blocknum, BlockNumber nblocks)
```

Calls `FileWriteback()` on the relevant segment files, advising the OS to asynchronously flush the specified range.

### mdnblocks()

Returns the total number of blocks across all segments of a fork by iterating through segments to find the last one.

## VFD Layer

The Virtual File Descriptor (VFD) layer (`src/backend/storage/file/fd.c`) manages a pool of OS file descriptors, since backends may need to access more files than the OS allows open simultaneously.

Key features:
- **LRU-based FD recycling**: When the limit of open files is reached, the least-recently-used VFD is closed, and its file descriptor is reused.
- **Transparent reopen**: When a VFD whose FD was recycled is accessed again, the file is transparently reopened.
- **Temporary file tracking**: Ensures temporary files are deleted at transaction end.

The buffer manager never interacts with the VFD layer directly; it always goes through smgr -> md -> VFD.

## Relation Forks

PostgreSQL stores different types of data for each relation in separate "forks":

| Fork | Number | Suffix | Purpose |
|------|--------|--------|---------|
| MAIN | 0 | (none) | Primary data (heap tuples, index entries) |
| FSM | 1 | `_fsm` | Free space map |
| VM | 2 | `_vm` | Visibility map |
| INIT | 3 | `_init` | Init fork for unlogged relations |

Each fork has its own set of segment files and its own cached size in [SMgrRelation](appendix_data_structures.md).

---

[<< WAL Integration](10_wal_integration.md) | [Index](index.md) | [Next: Data Movement and Durability >>](12_data_movement_and_durability.md)
