# GinPageOpaqueData

## Location
[src/include/access/ginblock.h:30-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/ginblock.h#L30-L37)

## Overview
GinPageOpaqueData is the page opaque data structure used in GIN (Generalized Inverted Index) pages, containing essential metadata for page navigation and content tracking.

## Definition

```c
typedef struct GinPageOpaqueData
{
	BlockNumber rightlink;		/* next page if any */
	OffsetNumber maxoff;		/* number of PostingItems on GIN_DATA &
								 * ~GIN_LEAF page. On GIN_LIST page, number of
								 * heap tuples. */
	uint16		flags;			/* see bit definitions below */
} GinPageOpaqueData;
```
## Detailed Description
GinPageOpaqueData serves as the opaque data structure for GIN index pages, providing essential page-level metadata. Unlike other PostgreSQL index types, GIN does not include a page ID word in its opaque data, relying instead on the distinctive 8-byte size for identification. This structure enables page linking through the rightlink field and tracks the number of items or tuples stored on the page through maxoff. The flags field stores various page state information using bit flags defined elsewhere in the GIN implementation.

The structure is designed to be compact at only 8 bytes total, which allows it to be reliably distinguished from other index types by size alone. This design choice was made when GIN was unique in using 8-byte special space, though SP-GiST (since 9.2) and BRIN (since 9.5) now also use 8-byte special space.

## Parameters / Member Variables
- `rightlink`: BlockNumber pointing to the next page in a sequence, used for page chaining and navigation
- `maxoff`: OffsetNumber indicating the count of items on the page - specifically PostingItems on GIN_DATA and non-GIN_LEAF pages, or heap tuples on GIN_LIST pages
- `flags`: uint16 bit field storing various page state flags and properties (specific bit definitions are defined elsewhere)
## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (PostgreSQL block numbering type)
  - OffsetNumber (PostgreSQL offset numbering type)
- Called from (representative examples):
  - GinPageOpaque (macro for accessing page opaque data)
  - [GinInitPage](GinInitPage.md) (page initialization)
  - GIN_PAGE_FREESIZE (free space calculation)
  - GinMaxItemSize (maximum item size calculation)
  - GinDataPageMaxDataSize (data page size calculation)
  - GinListPageSize (list page size calculation)

## Notes and Other Information
- The 8-byte size is critical for distinguishing GIN pages from other index types without requiring a page ID word
- The structure must maintain its size to preserve compatibility with the size-based identification mechanism
- As of PostgreSQL 9.2, SP-GiST also uses 8-byte special space, and BRIN does so as of 9.5, but this remains compatible as long as GIN doesn't use all high-order bits in the flags word
- The maxoff field has different meanings depending on the page type (PostingItems vs heap tuples)
- Located in src/include/access/ginblock.h at lines 30-37