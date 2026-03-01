# Appendix: Key Data Structures

[<< Glossary](appendix_glossary.md) | [Index](index.md) | [Next: GUC Parameters >>](appendix_guc_parameters.md)

---

## BufferDesc

Source: `src/include/storage/buf_internals.h`

```c
typedef struct BufferDesc
{
    BufferTag   tag;            /* ID of page contained in buffer */
    int         buf_id;         /* buffer's index number (from 0) */
    pg_atomic_uint32 state;     /* flags (10b) | usagecount (4b) | refcount (18b) */
    int         wait_backend_pgprocno;  /* backend of pin-count waiter */
    int         freeNext;       /* link in freelist chain */
    LWLock      content_lock;   /* to lock access to buffer contents */
} BufferDesc;
```

**Size:** Padded to 64 bytes (one cache line) via `BufferDescPadded`.

| Field | Description | Protected By |
|-------|-------------|-------------|
| `tag` | [BufferTag](#buffertag) identifying the page | Header spinlock (BM_LOCKED) |
| `buf_id` | Fixed slot number (0-based), never changes | Immutable after init |
| `state` | Atomic state word (see [state encoding](03_buffer_pool_architecture.md)) | CAS or BM_LOCKED |
| `wait_backend_pgprocno` | Backend waiting for pin-count-1 | BM_LOCKED |
| `freeNext` | Next buffer in free list | `buffer_strategy_lock` |
| `content_lock` | LWLock for page data access | Self (LWLock protocol) |

Documented in: [Buffer Pool Architecture](03_buffer_pool_architecture.md)

---

## BufferTag

Source: `src/include/storage/buf_internals.h`

```c
typedef struct buftag
{
    Oid         spcOid;         /* tablespace oid */
    Oid         dbOid;          /* database oid */
    RelFileNumber relNumber;    /* relation file number */
    ForkNumber  forkNum;        /* fork number (MAIN=0, FSM=1, VM=2, INIT=3) */
    BlockNumber blockNum;       /* blknum relative to begin of reln */
} BufferTag;
```

**Size:** 20 bytes.

Used as the hash key for the [buffer lookup table](04_buffer_lookup_and_hashtable.md). Contains all information needed to locate a block on disk without consulting catalog tables.

Documented in: [Buffer Pool Architecture](03_buffer_pool_architecture.md)

---

## PageHeaderData

Source: `src/include/storage/bufpage.h`

```c
typedef struct PageHeaderData
{
    PageXLogRecPtr pd_lsn;      /* 8 bytes: LSN of last WAL change */
    uint16      pd_checksum;    /* 2 bytes: page checksum */
    uint16      pd_flags;       /* 2 bytes: PD_HAS_FREE_LINES | PD_PAGE_FULL | PD_ALL_VISIBLE */
    LocationIndex pd_lower;     /* 2 bytes: offset to start of free space */
    LocationIndex pd_upper;     /* 2 bytes: offset to end of free space */
    LocationIndex pd_special;   /* 2 bytes: offset to start of special space */
    uint16      pd_pagesize_version; /* 2 bytes: page size (high 8) | version (low 8) */
    TransactionId pd_prune_xid; /* 4 bytes: oldest prunable XID */
    ItemIdData  pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* 4 bytes each: line pointer array */
} PageHeaderData;
```

**Fixed header size:** 24 bytes (`SizeOfPageHeaderData`).

Documented in: [Page Layout and Types](08_page_layout_and_types.md)

---

## BufferStrategyControl

Source: `src/backend/storage/buffer/freelist.c`

```c
typedef struct
{
    slock_t     buffer_strategy_lock;       /* protects freelist fields */
    pg_atomic_uint32 nextVictimBuffer;      /* clock sweep hand (atomic) */
    int         firstFreeBuffer;            /* head of free list (-1 = empty) */
    int         lastFreeBuffer;             /* tail of free list */
    uint32      completePasses;             /* full clock sweep cycles completed */
    pg_atomic_uint32 numBufferAllocs;       /* allocations since last reset */
    int         bgwprocno;                  /* bgwriter proc for wakeup, or -1 */
} BufferStrategyControl;
```

Located in shared memory. Single instance.

Documented in: [Buffer Replacement Policy](07_buffer_replacement_policy.md)

---

## SMgrRelationData

Source: `src/include/storage/smgr.h`

```c
typedef struct SMgrRelationData
{
    RelFileLocatorBackend smgr_rlocator;                   /* hash key */
    BlockNumber smgr_targblock;                            /* insertion target */
    BlockNumber smgr_cached_nblocks[MAX_FORKNUM + 1];     /* size cache per fork */
    int         smgr_which;                                /* always 0 (md) */
    int         md_num_open_segs[MAX_FORKNUM + 1];
    struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];
    int         pincount;
    dlist_node  node;
} SMgrRelationData;
```

Per-backend, hash table indexed by `RelFileLocatorBackend`. One per relation accessed.

Documented in: [Storage Manager](11_storage_manager.md)

---

## BufferAccessStrategyData

Source: `src/backend/storage/buffer/freelist.c`

```c
typedef struct BufferAccessStrategyData
{
    BufferAccessStrategyType btype;                    /* BAS_NORMAL, BAS_BULKREAD, etc. */
    int         nbuffers;                              /* ring size */
    int         current;                               /* current slot index */
    Buffer      buffers[FLEXIBLE_ARRAY_MEMBER];        /* ring of buffer numbers */
} BufferAccessStrategyData;
```

Per-backend, allocated by `GetAccessStrategy()`. Contains a circular ring of buffer numbers.

Documented in: [Buffer Replacement Policy](07_buffer_replacement_policy.md)

---

## WritebackContext

Source: `src/include/storage/buf_internals.h`

```c
typedef struct WritebackContext
{
    int        *max_pending;                              /* pointer to GUC */
    int         nr_pending;                               /* current pending count */
    PendingWriteback pending_writebacks[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;
```

Used by checkpoint, bgwriter, and backend to coalesce writeback advisories.

Documented in: [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md)

---

## PrivateRefCountEntry

Source: `src/backend/storage/buffer/bufmgr.c`

```c
typedef struct PrivateRefCountEntry
{
    Buffer      buffer;
    int32       refcount;
} PrivateRefCountEntry;
```

Per-backend, 8-entry array (one cache line) with hash table overflow. Tracks which buffers the current backend has pinned.

Documented in: [Page Concurrency Control](06_page_concurrency_control.md)

---

## ItemIdData (Line Pointer)

Source: `src/include/storage/itemid.h`

```c
typedef struct ItemIdData
{
    unsigned    lp_off:15,      /* offset to tuple (from page start) */
                lp_flags:2,     /* LP_UNUSED, LP_NORMAL, LP_REDIRECT, LP_DEAD */
                lp_len:15;      /* byte length of tuple */
} ItemIdData;
```

**Size:** 4 bytes. Uses 1-based numbering (`OffsetNumber`).

Documented in: [Page Layout and Types](08_page_layout_and_types.md)

---

## MdfdVec

Source: `src/backend/storage/smgr/md.c`

```c
typedef struct _MdfdVec
{
    File    mdfd_vfd;           /* virtual file descriptor */
    BlockNumber mdfd_segno;     /* segment number (0, 1, 2, ...) */
} MdfdVec;
```

Per-backend, per-fork array of open segment descriptors.

Documented in: [Storage Manager](11_storage_manager.md)

---

## Atomic State Word Layout

```
Bit 31    Bit 22    Bit 18    Bit 0
|---------|---------|---------|
| flags   | usage   | refcnt  |
| (10 bit)| (4 bit) | (18 bit)|
```

| Bits | Field | Range | Notes |
|------|-------|-------|-------|
| 31-22 | Flags | 10 bits | BM_PERMANENT, BM_CHECKPOINT_NEEDED, BM_PIN_COUNT_WAITER, BM_JUST_DIRTIED, BM_IO_ERROR, BM_IO_IN_PROGRESS, BM_TAG_VALID, BM_VALID, BM_DIRTY, BM_LOCKED |
| 21-18 | Usage count | 0-15 | Capped at BM_MAX_USAGE_COUNT (5) |
| 17-0 | Refcount | 0-262143 | Number of backends pinning this buffer |

Documented in: [Buffer Pool Architecture](03_buffer_pool_architecture.md)

---

[<< Glossary](appendix_glossary.md) | [Index](index.md) | [Next: GUC Parameters >>](appendix_guc_parameters.md)
