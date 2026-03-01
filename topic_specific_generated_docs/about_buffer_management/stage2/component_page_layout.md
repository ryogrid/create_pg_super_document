# Page Layout and Structure

## Overview

Every data page in PostgreSQL is a fixed-size block (default 8,192 bytes, configured at compile time via `BLCKSZ`). Pages use a "slotted page" layout with a fixed header, a variable-length array of line pointers (item identifiers) growing downward, and tuple data growing upward from the bottom. This chapter documents the `PageHeaderData` structure, the item pointer mechanism, and the key page manipulation functions.

Header definitions are in `src/include/storage/bufpage.h` (510 lines). Page operation implementations are in `src/backend/storage/page/bufpage.c`.

## Page Layout Diagram

From the comment in `src/include/storage/bufpage.h:22-76`:

```
+----------------+---------------------------------+
| PageHeaderData | linp1 linp2 linp3 ...           |
+-----------+----+---------------------------------+
| ... linpN |                                      |
+-----------+--------------------------------------+
|           ^ pd_lower                             |
|                                                  |
|             v pd_upper                           |
+-------------+------------------------------------+
|             | tupleN ...                         |
+-------------+------------------+-----------------+
|       ... tuple3 tuple2 tuple1 | "special space" |
+--------------------------------+-----------------+
                                  ^ pd_special
```

- **Header region** (fixed): `SizeOfPageHeaderData` = 24 bytes (offset of `pd_linp` array).
- **Line pointer array**: Grows downward from the header. Each entry is 4 bytes (`ItemIdData`).
- **Free space**: Between `pd_lower` (end of line pointers) and `pd_upper` (start of tuple data).
- **Tuple data**: Grows upward from `pd_upper` toward `pd_lower`.
- **Special space**: Optional access-method-specific data at the end of the page (e.g., btree opaque data).

## PageHeaderData

Source: `src/include/storage/bufpage.h:155-168`

```c
typedef struct PageHeaderData
{
    PageXLogRecPtr pd_lsn;      /* LSN: next byte after last byte of xlog
                                 * record for last change to this page */
    uint16      pd_checksum;    /* page checksum */
    uint16      pd_flags;       /* flag bits, see below */
    LocationIndex pd_lower;     /* offset to start of free space */
    LocationIndex pd_upper;     /* offset to end of free space */
    LocationIndex pd_special;   /* offset to start of special space */
    uint16      pd_pagesize_version;
    TransactionId pd_prune_xid; /* oldest prunable XID, or zero if none */
    ItemIdData  pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* line pointer array */
} PageHeaderData;
```

### Field Details

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `pd_lsn` | 0 | 8 bytes | LSN of last WAL record affecting this page. Stored as two 32-bit values (`PageXLogRecPtr`). Used by the buffer manager to enforce WAL-before-data. |
| `pd_checksum` | 8 | 2 bytes | Page checksum (if data checksums enabled). Zero is a valid checksum. |
| `pd_flags` | 10 | 2 bytes | Flag bits (see below). |
| `pd_lower` | 12 | 2 bytes | Byte offset to start of free space (end of line pointer array). |
| `pd_upper` | 14 | 2 bytes | Byte offset to end of free space (start of tuple data). |
| `pd_special` | 16 | 2 bytes | Byte offset to start of special space at page end. |
| `pd_pagesize_version` | 18 | 2 bytes | Page size (high 8 bits) and layout version (low 8 bits). |
| `pd_prune_xid` | 20 | 4 bytes | Oldest XID that might make a tuple prunable. Hint for heap pruning. |
| `pd_linp[]` | 24 | 4 bytes each | Line pointer array (variable length). |

### Page Flags

Source: `src/include/storage/bufpage.h:184-189`

```c
#define PD_HAS_FREE_LINES  0x0001  /* are there any unused line pointers? */
#define PD_PAGE_FULL        0x0002  /* not enough free space for new tuple? */
#define PD_ALL_VISIBLE      0x0004  /* all tuples on page visible to everyone */
#define PD_VALID_FLAG_BITS  0x0007  /* OR of all valid pd_flags bits */
```

- **PD_HAS_FREE_LINES**: Hint that there are `LP_UNUSED` line pointers. Changes are not WAL-logged.
- **PD_PAGE_FULL**: Set when an UPDATE cannot find space; triggers pruning on next access.
- **PD_ALL_VISIBLE**: All tuples visible to all transactions. Used for visibility map integration and index-only scans.

### Page Size and Version Encoding

```c
/* From src/include/storage/bufpage.h:273-277 */
static inline Size
PageGetPageSize(Page page)
{
    return (Size) (((PageHeader) page)->pd_pagesize_version & (uint16) 0xFF00);
}

static inline uint8
PageGetPageLayoutVersion(Page page)
{
    return (((PageHeader) page)->pd_pagesize_version & 0x00FF);
}
```

Page size must be a multiple of 256 (leaving low 8 bits for version). Current layout version is `PG_PAGE_LAYOUT_VERSION = 4` (since PostgreSQL 8.3).

### Page LSN

The `pd_lsn` field uses a split 64-bit representation for historical reasons:

```c
/* From src/include/storage/bufpage.h:94-107 */
typedef struct
{
    uint32      xlogid;     /* high bits */
    uint32      xrecoff;    /* low bits */
} PageXLogRecPtr;

static inline XLogRecPtr
PageXLogRecPtrGet(PageXLogRecPtr val)
{
    return (uint64) val.xlogid << 32 | val.xrecoff;
}
```

Accessor functions:

```c
/* From src/include/storage/bufpage.h:383-392 */
static inline XLogRecPtr PageGetLSN(Page page)
{
    return PageXLogRecPtrGet(((PageHeader) page)->pd_lsn);
}
static inline void PageSetLSN(Page page, XLogRecPtr lsn)
{
    PageXLogRecPtrSet(((PageHeader) page)->pd_lsn, lsn);
}
```

## Line Pointers (Item Identifiers)

Each tuple on a page is referenced by a line pointer (`ItemIdData`, defined in `src/include/storage/itemid.h`). Line pointers are 4 bytes each and contain:

- `lp_off` (15 bits): Byte offset of the item within the page.
- `lp_flags` (2 bits): Status flags (`LP_UNUSED`, `LP_NORMAL`, `LP_REDIRECT`, `LP_DEAD`).
- `lp_len` (15 bits): Byte length of the item.

Line pointers use 1-based numbering (`OffsetNumber` starts at 1):

```c
/* From src/include/storage/bufpage.h:240-244 */
static inline ItemId
PageGetItemId(Page page, OffsetNumber offsetNumber)
{
    return &((PageHeader) page)->pd_linp[offsetNumber - 1];
}
```

## Core Page Operations

### PageInit()

Source: `src/backend/storage/page/bufpage.c`

```c
void PageInit(Page page, Size pageSize, Size specialSize)
```

Initializes a page with an empty structure:
- Zeroes the entire page.
- Sets `pd_lower` to `SizeOfPageHeaderData` (24).
- Sets `pd_upper` to `pageSize - specialSize`.
- Sets `pd_special` to `pageSize - specialSize`.
- Sets `pd_pagesize_version` to encode the page size and version 4.

After initialization: `pd_lower = 24`, `pd_upper = pd_special`, free space = pd_upper - pd_lower.

### PageAddItemExtended()

Source: `src/backend/storage/page/bufpage.c`

```c
OffsetNumber PageAddItemExtended(Page page, Item item, Size size,
                                 OffsetNumber offsetNumber, int flags)
```

Adds an item to a page. The item data is copied into the page at the appropriate location.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `page` | `Page` | Target page |
| `item` | `Item` | Pointer to item data to add |
| `size` | `Size` | Size of item data |
| `offsetNumber` | `OffsetNumber` | Desired offset (or `InvalidOffsetNumber` for append) |
| `flags` | `int` | `PAI_OVERWRITE` and/or `PAI_IS_HEAP` |

**Logic flow:**

1. If `offsetNumber` is `InvalidOffsetNumber`, find a free line pointer slot (scan for `LP_UNUSED`) or append a new one.
2. Ensure there is enough free space between `pd_lower` and `pd_upper` for both the new line pointer (if needed) and the item data.
3. Decrement `pd_upper` by the aligned item size.
4. Copy item data to the new `pd_upper` location.
5. Set the line pointer to point to the new item.
6. Increment `pd_lower` if a new line pointer was added.

**Returns:** The `OffsetNumber` where the item was placed, or `InvalidOffsetNumber` if the page has no room.

### PageRepairFragmentation()

Source: `src/backend/storage/page/bufpage.c`

```c
void PageRepairFragmentation(Page page)
```

Compacts the tuple data area, eliminating gaps left by deleted tuples. After compaction, all live tuples are contiguous at the end of the page, and `pd_upper` is updated to reflect the new start of tuple data. Line pointer offsets are updated to point to the new locations.

This is called after tuple deletion (e.g., by VACUUM or HOT pruning) to reclaim space.

### PageGetFreeSpace()

Source: `src/backend/storage/page/bufpage.c`

```c
Size PageGetFreeSpace(Page page)
```

Returns the amount of free space available for storing data. This accounts for the space between `pd_lower` and `pd_upper`, minus the overhead of a new line pointer (4 bytes).

### PageGetHeapFreeSpace()

```c
Size PageGetHeapFreeSpace(Page page)
```

Like `PageGetFreeSpace()` but also accounts for the per-tuple overhead needed by heap access methods (tuple header alignment).

## Page Verification and Checksums

### PageIsVerifiedExtended()

Source: `src/backend/storage/page/bufpage.c`

```c
bool PageIsVerifiedExtended(Page page, BlockNumber blkno, int flags)
```

Verifies page integrity. Checks:

1. If the page is all zeros, it is considered valid (newly-allocated page).
2. If data checksums are enabled, computes and compares the checksum.
3. Validates `pd_lower`, `pd_upper`, and `pd_special` are within bounds and properly ordered.

Flags: `PIV_LOG_WARNING` (log on failure), `PIV_REPORT_STAT` (update pgstat counters).

### PageSetChecksumCopy()

Source: `src/backend/storage/page/bufpage.c`

```c
char *PageSetChecksumCopy(Page page, BlockNumber blkno)
```

Computes the page checksum and returns a pointer to a copy of the page with the checksum set. **A copy is used** because hint bit updates can modify the page concurrently (under only a shared content lock), so modifying the checksum in-place could produce an inconsistent page image on disk.

```c
/* Critical: copy-on-write for checksum safety */
static char *pageCopy = NULL;
if (pageCopy == NULL)
    pageCopy = MemoryContextAllocAligned(TopMemoryContext, BLCKSZ,
                                         PG_IO_ALIGN_SIZE, 0);
memcpy(pageCopy, (char *) page, BLCKSZ);
((PageHeader) pageCopy)->pd_checksum = pg_checksum_page(pageCopy, blkno);
return pageCopy;
```

### PageSetChecksumInplace()

```c
void PageSetChecksumInplace(Page page, BlockNumber blkno)
```

Sets the checksum directly in the page buffer. Used only when the caller holds an exclusive content lock (guaranteeing no concurrent modifications).

## Constants

| Constant | Value | Source |
|----------|-------|--------|
| `SizeOfPageHeaderData` | 24 bytes | `offsetof(PageHeaderData, pd_linp)` |
| `PG_PAGE_LAYOUT_VERSION` | 4 | `src/include/storage/bufpage.h:203` |
| `PG_DATA_CHECKSUM_VERSION` | 1 | `src/include/storage/bufpage.h:204` |
| `BLCKSZ` | 8192 (default) | Compile-time configuration |
| `MaxHeapTuplesPerPage` | ~291 | Derived from BLCKSZ and minimum tuple size |
