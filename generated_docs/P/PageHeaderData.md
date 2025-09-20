# PageHeaderData

## Location
[src/include/storage/bufpage.h:155-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L155-L168)

## Overview
PageHeaderData is the fundamental structure that defines the header layout for all PostgreSQL database pages, containing metadata for space management, logging, and tuple organization within a page.

## Definition

```c
typedef struct PageHeaderData
{
	/* XXX LSN is member of *any* block, not only page-organized ones */
	PageXLogRecPtr pd_lsn;		/* LSN: next byte after last byte of xlog
								 * record for last change to this page */
	uint16		pd_checksum;	/* checksum */
	uint16		pd_flags;		/* flag bits, see below */
	LocationIndex pd_lower;		/* offset to start of free space */
	LocationIndex pd_upper;		/* offset to end of free space */
	LocationIndex pd_special;	/* offset to start of special space */
	uint16		pd_pagesize_version;
	TransactionId pd_prune_xid; /* oldest prunable XID, or zero if none */
	ItemIdData	pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* line pointer array */
} PageHeaderData;
```
## Detailed Description
PageHeaderData serves as the common header structure for all PostgreSQL pages, providing essential metadata for page management, WAL (Write-Ahead Logging) consistency, and space organization. This structure is present at the beginning of every page in PostgreSQL's storage system, enabling the buffer manager to track page state, enforce WAL ordering rules, and manage free space efficiently.

The structure supports PostgreSQL's fundamental storage architecture by maintaining information about the page's modification history (LSN), integrity verification (checksum), space utilization (lower/upper bounds), and tuple organization (line pointer array). The design accommodates both heap pages and index pages through the flexible special space mechanism.

## Parameters / Member Variables
- `pd_lsn`: Log Sequence Number identifying the WAL record for the last modification to this page; used by buffer manager to enforce "write xlog before data" rule
- `pd_checksum`: Page checksum for data integrity verification; zero is a valid checksum value, and the field may be unset if checksums are disabled
- `pd_flags`: Bit flags for various page states and properties (specific flag definitions found elsewhere in the codebase)
- `pd_lower`: Byte offset from page start to the beginning of free space area; marks the end of the line pointer array
- `pd_upper`: Byte offset from page start to the end of free space area; marks the beginning of actual tuple data (growing backwards)
- `pd_special`: Byte offset from page start to the beginning of special space area; used for index-specific data structures
- `pd_pagesize_version`: Combined field storing page size (in multiples of 256 bytes) and page layout version number in a single uint16
- `pd_prune_xid`: Transaction ID of the oldest potentially prunable tuple on the page; used as a hint for HOT (Heap-Only Tuples) pruning optimization
- `pd_linp[FLEXIBLE_ARRAY_MEMBER]`: Flexible array of line pointers (ItemIdData) that reference individual tuples or items on the page
## Dependencies
- Functions called/Symbols referenced:
  - PageXLogRecPtr
  - LocationIndex
  - [ItemIdData](../I/ItemIdData.md)
  - TransactionId
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - PageHeader (typedef pointer to PageHeaderData)
  - SizeOfPageHeaderData (size calculation macro)
  - [rewriteVisibilityMap](../r/rewriteVisibilityMap.md) (in pg_upgrade utilities)

## Notes and Other Information
- Page sizes must be multiples of 256 bytes, with the low 8 bits of pd_pagesize_version used for version numbering
- Maximum supported page size is 32KB due to 15-bit limitations in ItemIdData offset/length fields
- The LSN field is critical for WAL consistency and prevents dirty pages from being written before their corresponding WAL records
- Checksum validation relies on external mechanisms since there are no on-page flags indicating checksum validity
- The structure layout is optimized for both heap and index pages, with special space providing flexibility for index-specific metadata
- Historical compatibility is maintained through the version numbering scheme, allowing pre-7.3 databases to be treated as version 0