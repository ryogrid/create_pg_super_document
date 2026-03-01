# Storage Manager Layer

## Overview

The storage manager (smgr) provides a pluggable abstraction layer between the buffer manager and the physical file system. It mediates all I/O operations on relation data files, translating buffer-manager requests (read page, write page, extend relation) into file system operations. Although the API supports multiple storage backends via function pointer tables, PostgreSQL currently only implements one: the "magnetic disk" manager (`md.c`), which manages files using the Virtual File Descriptor (VFD) layer.

The storage manager interface is defined in `src/include/storage/smgr.h` (131 lines) and implemented in `src/backend/storage/smgr/smgr.c`. The md backend is in `src/backend/storage/smgr/md.c` (1829 lines).

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

Source: `src/include/storage/smgr.h:34-69`

```c
typedef struct SMgrRelationData
{
    /* Hash key: relation physical identifier */
    RelFileLocatorBackend smgr_rlocator;

    BlockNumber smgr_targblock;                    /* current insertion target block */
    BlockNumber smgr_cached_nblocks[MAX_FORKNUM + 1]; /* cached relation size per fork */

    int         smgr_which;     /* storage manager selector (always 0 = md) */

    /* md.c state: per-fork arrays of open segments */
    int         md_num_open_segs[MAX_FORKNUM + 1];
    struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];

    /* Pinning support */
    int         pincount;
    dlist_node  node;           /* link in unpinned list */
} SMgrRelationData;
```

Each backend maintains a hash table of `SMgrRelation` objects, one per relation accessed. These are essentially cached file handles with the following lifecycle:

- **Created** by `smgropen()` on first access.
- **Pinned** by the relcache to prevent premature destruction.
- **Unpinned** entries are destroyed at end of transaction by `AtEOXact_SMgr()`.
- **Cached sizes**: `smgr_cached_nblocks[]` avoids repeated `lseek()` calls during recovery.

## f_smgr Function Pointer Table

Source: `src/backend/storage/smgr/smgr.c:74-105`

```c
typedef struct f_smgr
{
    void (*smgr_init)(void);
    void (*smgr_shutdown)(void);
    void (*smgr_open)(SMgrRelation reln);
    void (*smgr_close)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_create)(SMgrRelation reln, ForkNumber forknum, bool isRedo);
    bool (*smgr_exists)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_unlink)(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo);
    void (*smgr_extend)(SMgrRelation reln, ForkNumber forknum,
                        BlockNumber blocknum, const void *buffer, bool skipFsync);
    void (*smgr_zeroextend)(SMgrRelation reln, ForkNumber forknum,
                            BlockNumber blocknum, int nblocks, bool skipFsync);
    bool (*smgr_prefetch)(SMgrRelation reln, ForkNumber forknum,
                          BlockNumber blocknum, int nblocks);
    void (*smgr_readv)(SMgrRelation reln, ForkNumber forknum,
                       BlockNumber blocknum, void **buffers, BlockNumber nblocks);
    void (*smgr_writev)(SMgrRelation reln, ForkNumber forknum,
                        BlockNumber blocknum, const void **buffers,
                        BlockNumber nblocks, bool skipFsync);
    void (*smgr_writeback)(SMgrRelation reln, ForkNumber forknum,
                           BlockNumber blocknum, BlockNumber nblocks);
    BlockNumber (*smgr_nblocks)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_truncate)(SMgrRelation reln, ForkNumber forknum,
                          BlockNumber old_blocks, BlockNumber nblocks);
    void (*smgr_immedsync)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_registersync)(SMgrRelation reln, ForkNumber forknum);
} f_smgr;
```

Currently there is exactly one backend:

```c
static const f_smgr smgrsw[] = {
    {
        .smgr_init = mdinit,
        .smgr_open = mdopen,
        .smgr_close = mdclose,
        .smgr_readv = mdreadv,
        .smgr_writev = mdwritev,
        /* ... all md* functions ... */
    }
};
```

## Core SMgr API

### smgropen()

Source: `src/backend/storage/smgr/smgr.c:197-243`

```c
SMgrRelation smgropen(RelFileLocator rlocator, ProcNumber backend)
```

Returns an `SMgrRelation` object for the given relation file locator, creating one if it does not exist. The object is valid for the lifetime of the current transaction.

**Implementation:**

```c
/* Look up or create hash table entry */
reln = hash_search(SMgrRelationHash, &brlocator, HASH_ENTER, &found);

if (!found)
{
    reln->smgr_targblock = InvalidBlockNumber;
    for (int i = 0; i <= MAX_FORKNUM; ++i)
        reln->smgr_cached_nblocks[i] = InvalidBlockNumber;
    reln->smgr_which = 0;  /* md.c */
    reln->pincount = 0;
    dlist_push_tail(&unpinned_relns, &reln->node);
    smgrsw[reln->smgr_which].smgr_open(reln);
}
return reln;
```

Note that `smgropen()` does NOT open any physical files. The `mdopen()` call is essentially a no-op that initializes the per-fork segment arrays. Actual file descriptors are obtained lazily on first I/O.

### smgrread() / smgrreadv()

Source: `src/include/storage/smgr.h:116-121`

```c
static inline void
smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
         void *buffer)
{
    smgrreadv(reln, forknum, blocknum, &buffer, 1);
}
```

`smgrread()` is an inline wrapper around `smgrreadv()`, which dispatches to `mdreadv()`.

### smgrwrite() / smgrwritev()

```c
static inline void
smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
          const void *buffer, bool skipFsync)
{
    smgrwritev(reln, forknum, blocknum, &buffer, 1, skipFsync);
}
```

Writes a page to the kernel page cache. The `skipFsync` parameter controls whether the write is registered for deferred fsync (needed for WAL-logged operations where fsync is batched at checkpoint).

### smgrextend()

```c
void smgrextend(SMgrRelation reln, ForkNumber forknum,
                BlockNumber blocknum, const void *buffer, bool skipFsync)
```

Extends a relation by writing a new block at the specified position. This is used when the relation needs to grow (e.g., `ExtendBufferedRel()`).

### smgrnblocks()

```c
BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum)
```

Returns the number of blocks in the specified fork. This may involve an `lseek()` call to determine file size. The result is cached in `smgr_cached_nblocks[forknum]`.

### smgrwriteback()

```c
void smgrwriteback(SMgrRelation reln, ForkNumber forknum,
                   BlockNumber blocknum, BlockNumber nblocks)
```

Advises the kernel to write back dirty pages for the specified block range. On Linux, this maps to `sync_file_range()` or `posix_fadvise(POSIX_FADV_DONTNEED)`.

### smgrimmedsync()

```c
void smgrimmedsync(SMgrRelation reln, ForkNumber forknum)
```

Immediately fsync the relation fork. Used for operations that bypass WAL (e.g., creating non-logged relation files).

## MD Layer (Magnetic Disk Manager)

### Segment Layout

MD splits relation forks into segments of at most `RELSEG_SIZE` blocks (default: 131,072 blocks = 1 GB per segment). This avoids issues with file size limits on some platforms. Segment files are named:

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

Source: `src/backend/storage/smgr/md.c:806-918`

```c
void mdreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
             void **buffers, BlockNumber nblocks)
```

Reads one or more blocks from a relation fork. Key implementation details:

1. Locates the correct segment via `_mdfd_getseg()`.
2. Computes the offset within the segment: `seekpos = (off_t) BLCKSZ * (blocknum % RELSEG_SIZE)`.
3. Uses `FileReadV()` for vectorized I/O (scatter read with `preadv()`).
4. Handles short reads with an inner retry loop.
5. On EOF, either zeros the page (if `zero_damaged_pages` or `InRecovery`) or raises ERROR.

### mdwritev()

Similar to `mdreadv()` but calls `FileWriteV()` for vectorized writes. If `skipFsync` is false, registers the file for fsync via `register_dirty_segment()`.

### mdextend()

Extends the relation by writing a single block at the end. If the new block falls in a new segment, creates the segment file first.

### mdwriteback()

```c
void mdwriteback(SMgrRelation reln, ForkNumber forknum,
                 BlockNumber blocknum, BlockNumber nblocks)
```

Calls `FileWriteback()` on the relevant segment files. This advises the OS to asynchronously flush the specified range to disk, reducing the work needed at the next fsync.

### mdnblocks()

Returns the total number of blocks across all segments of a fork. Iterates through segments to find the last one, then computes the total from the last segment's size.

## VFD Layer

The Virtual File Descriptor (VFD) layer (`src/backend/storage/file/fd.c`) manages a pool of OS file descriptors, since backends may need to access more files than the OS allows open simultaneously.

Key features:
- **LRU-based FD recycling**: When the limit of open files is reached, the least-recently-used VFD is closed, and its file descriptor is reused.
- **Transparent reopen**: When a VFD whose FD was recycled is accessed again, the file is transparently reopened.
- **Temporary file tracking**: Ensures temporary files are deleted at transaction end.

The buffer manager never interacts with the VFD layer directly; it always goes through smgr -> md -> VFD.
