# Page Layout and Structure

[<< Buffer Replacement Policy](07_buffer_replacement_policy.md) | [Index](index.md) | [Next: Dirty Buffer and Writeback >>](09_dirty_buffer_and_writeback.md)

---

## Overview

Every data page in PostgreSQL is a fixed-size block (default 8,192 bytes, configured at compile time via `BLCKSZ`). Pages use a "slotted page" layout with a fixed header, a variable-length array of line pointers (item identifiers) growing downward, and tuple data growing upward from the bottom.

Header definitions are in `src/include/storage/bufpage.h`. Page operation implementations are in `src/backend/storage/page/bufpage.c`.

See diagram: [page_layout.mermaid](../diagrams/page_layout.mermaid)

## Page Layout Diagram

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

- **Header region** (fixed): `SizeOfPageHeaderData` = 24 bytes.
- **Line pointer array**: Grows downward from the header. Each entry is 4 bytes (`ItemIdData`).
- **Free space**: Between `pd_lower` (end of line pointers) and `pd_upper` (start of tuple data).
- **Tuple data**: Grows upward from `pd_upper` toward `pd_lower`.
- **Special space**: Optional access-method-specific data at the end of the page (e.g., btree opaque data).

## PageHeaderData

Source: `src/include/storage/bufpage.h:155`

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

See [Data Structures Appendix](appendix_data_structures.md) for the full annotated definition.

### Field Details

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `pd_lsn` | 0 | 8 bytes | LSN of last WAL record affecting this page. Used by the buffer manager to enforce [WAL-before-data](10_wal_integration.md). |
| `pd_checksum` | 8 | 2 bytes | Page checksum (if data checksums enabled). |
| `pd_flags` | 10 | 2 bytes | Flag bits (see below). |
| `pd_lower` | 12 | 2 bytes | Byte offset to start of free space (end of line pointer array). |
| `pd_upper` | 14 | 2 bytes | Byte offset to end of free space (start of tuple data). |
| `pd_special` | 16 | 2 bytes | Byte offset to start of special space at page end. |
| `pd_pagesize_version` | 18 | 2 bytes | Page size (high 8 bits) and layout version (low 8 bits). |
| `pd_prune_xid` | 20 | 4 bytes | Oldest XID that might make a tuple prunable. |
| `pd_linp[]` | 24 | 4 bytes each | Line pointer array (variable length). |

### Page Flags

Source: `src/include/storage/bufpage.h:184`

```c
#define PD_HAS_FREE_LINES  0x0001  /* are there any unused line pointers? */
#define PD_PAGE_FULL        0x0002  /* not enough free space for new tuple? */
#define PD_ALL_VISIBLE      0x0004  /* all tuples on page visible to everyone */
#define PD_VALID_FLAG_BITS  0x0007  /* OR of all valid pd_flags bits */
```

- **PD_HAS_FREE_LINES**: Hint that there are `LP_UNUSED` line pointers. Changes are not WAL-logged.
- **PD_PAGE_FULL**: Set when an UPDATE cannot find space; triggers pruning on next access.
- **PD_ALL_VISIBLE**: All tuples visible to all transactions. Used for visibility map integration and index-only scans.

### Page LSN

The `pd_lsn` field uses a split 64-bit representation for historical reasons:

```c
typedef struct
{
    uint32      xlogid;     /* high bits */
    uint32      xrecoff;    /* low bits */
} PageXLogRecPtr;
```

Accessor functions:

```c
static inline XLogRecPtr PageGetLSN(Page page)
{
    return PageXLogRecPtrGet(((PageHeader) page)->pd_lsn);
}
static inline void PageSetLSN(Page page, XLogRecPtr lsn)
{
    PageXLogRecPtrSet(((PageHeader) page)->pd_lsn, lsn);
}
```

See [WAL Integration](10_wal_integration.md) for how the LSN enforces the WAL-before-data rule.

## Line Pointers (Item Identifiers)

Each tuple on a page is referenced by a line pointer (`ItemIdData`, defined in `src/include/storage/itemid.h`). Line pointers are 4 bytes each and contain:

- `lp_off` (15 bits): Byte offset of the item within the page.
- `lp_flags` (2 bits): Status flags (`LP_UNUSED`, `LP_NORMAL`, `LP_REDIRECT`, `LP_DEAD`).
- `lp_len` (15 bits): Byte length of the item.

Line pointers use 1-based numbering (`OffsetNumber` starts at 1):

```c
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

Initializes a page with an empty structure: zeroes the entire page, sets `pd_lower` to 24 (`SizeOfPageHeaderData`), sets `pd_upper` and `pd_special` to `pageSize - specialSize`, and encodes the page size and version 4 in `pd_pagesize_version`.

### PageAddItemExtended()

Source: `src/backend/storage/page/bufpage.c`

```c
OffsetNumber PageAddItemExtended(Page page, Item item, Size size,
                                 OffsetNumber offsetNumber, int flags)
```

Adds an item to a page. The item data is copied into the page at the appropriate location.

**Logic flow:**

1. If `offsetNumber` is `InvalidOffsetNumber`, find a free line pointer slot or append a new one.
2. Ensure there is enough free space between `pd_lower` and `pd_upper`.
3. Decrement `pd_upper` by the aligned item size.
4. Copy item data to the new `pd_upper` location.
5. Set the line pointer to point to the new item.
6. Increment `pd_lower` if a new line pointer was added.

**Returns:** The `OffsetNumber` where the item was placed, or `InvalidOffsetNumber` if the page has no room.

### PageRepairFragmentation()

```c
void PageRepairFragmentation(Page page)
```

Compacts the tuple data area, eliminating gaps left by deleted tuples. After compaction, all live tuples are contiguous at the end of the page. Called after tuple deletion (e.g., by [VACUUM](14_access_method_integration.md) or HOT pruning).

### PageGetFreeSpace() / PageGetHeapFreeSpace()

```c
Size PageGetFreeSpace(Page page)
Size PageGetHeapFreeSpace(Page page)
```

Returns the amount of free space available for storing data, accounting for the space between `pd_lower` and `pd_upper` and line pointer overhead.

## Page Verification and Checksums

### PageIsVerifiedExtended()

Source: `src/backend/storage/page/bufpage.c`

```c
bool PageIsVerifiedExtended(Page page, BlockNumber blkno, int flags)
```

Verifies page integrity:

1. If the page is all zeros, it is considered valid (newly-allocated page).
2. If data checksums are enabled, computes and compares the checksum.
3. Validates `pd_lower`, `pd_upper`, and `pd_special` are within bounds and properly ordered.

This function is called by [WaitReadBuffers()](05_buffer_access_protocol.md) after every disk read.

### PageSetChecksumCopy()

```c
char *PageSetChecksumCopy(Page page, BlockNumber blkno)
```

Computes the page checksum and returns a pointer to a **copy** of the page with the checksum set. A copy is used because hint bit updates can modify the page concurrently (under only a shared content lock), so modifying the checksum in-place could produce an inconsistent page image on disk. See [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) and [Deep Dives](15_deep_dives.md) for more on checksum handling.

### PageSetChecksumInplace()

```c
void PageSetChecksumInplace(Page page, BlockNumber blkno)
```

Sets the checksum directly in the page buffer. Used only when the caller holds an exclusive content lock (guaranteeing no concurrent modifications).

## Constants

| Constant | Value | Source |
|----------|-------|--------|
| `SizeOfPageHeaderData` | 24 bytes | `offsetof(PageHeaderData, pd_linp)` |
| `PG_PAGE_LAYOUT_VERSION` | 4 | `src/include/storage/bufpage.h` |
| `PG_DATA_CHECKSUM_VERSION` | 1 | `src/include/storage/bufpage.h` |
| `BLCKSZ` | 8192 (default) | Compile-time configuration |
| `MaxHeapTuplesPerPage` | ~291 | Derived from BLCKSZ and minimum tuple size |

---

[<< Buffer Replacement Policy](07_buffer_replacement_policy.md) | [Index](index.md) | [Next: Dirty Buffer and Writeback >>](09_dirty_buffer_and_writeback.md)
