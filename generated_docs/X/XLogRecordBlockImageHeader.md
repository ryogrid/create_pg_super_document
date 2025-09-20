# XLogRecordBlockImageHeader

## Location
[src/include/access/xlogrecord.h:141-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogrecord.h#L141-L151)

## Overview
XLogRecordBlockImageHeader contains additional header information for full-page images included in WAL records, supporting both hole removal optimization and optional compression of page images.

## Definition

```c
typedef struct XLogRecordBlockImageHeader
{
	uint16		length;			/* number of page image bytes */
	uint16		hole_offset;	/* number of bytes before "hole" */
	uint8		bimg_info;		/* flag bits, see below */

	/*
	 * If BKPIMAGE_HAS_HOLE and BKPIMAGE_COMPRESSED(), an
	 * XLogRecordBlockCompressHeader struct follows.
	 */
} XLogRecordBlockImageHeader;
```
## Detailed Description
XLogRecordBlockImageHeader provides metadata for full-page images stored in WAL records when BKPBLOCK_HAS_IMAGE is set. It implements two key optimizations: hole removal and optional compression. The hole removal optimization takes advantage of the fact that PostgreSQL data pages typically contain an unused "hole" in the middle filled with zeros, which can be omitted from WAL storage. When WAL compression is enabled, the structure supports additional compression of page images after hole removal, further reducing WAL volume at the cost of CPU overhead during logging.

## Parameters / Member Variables
- `length`: Total number of bytes in the page image data as stored in the WAL record (after hole removal and compression if applicable)
- `hole_offset`: Number of bytes from the start of the page before the "hole" begins, used to reconstruct the original page layout
- `bimg_info`: Flag bits containing information about compression status, hole presence, and other image properties
## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)

- Called from (representative examples):
  - [XLogRecordAssemble](XLogRecordAssemble.md)
  - SizeOfXLogRecordBlockImageHeader

## Notes and Other Information
- This header is only present when BKPBLOCK_HAS_IMAGE flag is set in the corresponding XLogRecordBlockHeader
- Supports PostgreSQL's hole removal optimization where zero-filled regions are omitted from WAL storage
- When compression is enabled and beneficial, the original page image is compressed after hole removal
- If compression doesn't provide sufficient space savings, the uncompressed version is stored instead
- When both hole and compression are present, an XLogRecordBlockCompressHeader follows this structure
- The hole optimization reduces WAL volume significantly since most pages have substantial unused space
- Part of PostgreSQL's efficient WAL storage system that balances storage space with recovery performance