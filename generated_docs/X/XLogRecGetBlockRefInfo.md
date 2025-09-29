# XLogRecGetBlockRefInfo

## Location
[src/backend/access/rmgrdesc/xlogdesc.c:231-353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/xlogdesc.c#L231-L353)

## Overview
Generates formatted information about all block references contained in a WAL (Write-Ahead Log) record, including details about full-page images and compression.

## Definition

```c
void
XLogRecGetBlockRefInfo(XLogReaderState *record, bool pretty,
					   bool detailed_format, StringInfo buf,
					   uint32 *fpi_len)
```
## Detailed Description
This function iterates through all block references in a WAL record and generates human-readable information about each block. It provides two formatting modes: detailed format (showing comprehensive block information) and short format (showing minimal block information). The function also tracks full-page image (FPI) data and compression details.

For each block reference, the function extracts the relation file locator (tablespace, database, relation number), fork type, and block number. When full-page images are present, it provides information about compression methods (pglz, lz4, zstd), hole information, and whether the image is for WAL verification purposes.

The function is commonly used by WAL analysis tools like pg_waldump and pg_walinspect to provide detailed information about WAL record contents for debugging and analysis purposes.

## Parameters / Member Variables
- `record`: XLogReaderState containing the WAL record to analyze
- `pretty`: Boolean flag to enable pretty-printing with proper formatting and newlines
- `detailed_format`: Boolean flag to enable detailed format (true) vs short format (false)
- `buf`: StringInfo buffer where the formatted block reference information will be appended
- `fpi_len`: Optional pointer to uint32 that will be updated with total full-page image length

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecMaxBlockId (gets maximum block ID in record)
  - [XLogRecGetBlockTagExtended](XLogRecGetBlockTagExtended.md) (extracts block tag information)
  - XLogRecHasBlockImage (checks if block has full-page image)
  - XLogRecGetBlock (gets block reference data)
  - XLogRecBlockImageApply (checks if block image should be applied)
  - Various BKPIMAGE_* compression constants
  - [appendStringInfo](../a/appendStringInfo.md), appendStringInfoChar, appendStringInfoString (string buffer operations)
- Called from (representative examples):
  - [XLogDumpDisplayRecord](XLogDumpDisplayRecord.md) (pg_waldump utility)
  - pg_walinspect extension functions

## Notes and Other Information
- Supports both detailed and short format output modes for different use cases
- Handles multiple compression methods for full-page images: pglz, lz4, zstd
- Provides information about hole offset and length within compressed full-page images
- Distinguishes between regular full-page writes and those used for WAL verification
- Can optionally calculate and return the total size of full-page image data
- Used extensively by PostgreSQL's WAL analysis and debugging tools
- The function safely handles records with no block references
- Fork names are displayed using the forkNames array for human readability

## Simplified Source

```c
void XLogRecGetBlockRefInfo(XLogReaderState *record, bool pretty,
                           bool detailed_format, StringInfo buf,
                           uint32 *fpi_len)
{
    int block_id;

    Assert(record != NULL);

    if (detailed_format && pretty)
        appendStringInfoChar(buf, '\n');

    // Iterate through all block references in the record
    for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
    {
        RelFileLocator rlocator;
        ForkNumber forknum;
        BlockNumber blk;

        // Get block tag information
        if (!XLogRecGetBlockTagExtended(record, block_id,
                                       &rlocator, &forknum, &blk, NULL))
            continue;

        if (detailed_format)
        {
            // Detailed format: show comprehensive block information
            if (pretty)
                appendStringInfoChar(buf, '\t');
            else if (block_id > 0)
                appendStringInfoChar(buf, ' ');

            appendStringInfo(buf,
                           "blkref #%d: rel %u/%u/%u fork %s blk %u",
                           block_id,
                           rlocator.spcOid, rlocator.dbOid, rlocator.relNumber,
                           forkNames[forknum], blk);

            // Handle full-page image information
            if (XLogRecHasBlockImage(record, block_id))
            {
                uint8 bimg_info = XLogRecGetBlock(record, block_id)->bimg_info;

                // Update FPI length if requested
                if (fpi_len)
                    *fpi_len += XLogRecGetBlock(record, block_id)->bimg_len;

                // Handle compressed images
                if (BKPIMAGE_COMPRESSED(bimg_info))
                {
                    const char *method;

                    // Determine compression method
                    if ((bimg_info & BKPIMAGE_COMPRESS_PGLZ) != 0)
                        method = "pglz";
                    else if ((bimg_info & BKPIMAGE_COMPRESS_LZ4) != 0)
                        method = "lz4";
                    else if ((bimg_info & BKPIMAGE_COMPRESS_ZSTD) != 0)
                        method = "zstd";
                    else
                        method = "unknown";

                    appendStringInfo(buf,
                                   " (FPW%s); hole: offset: %u, length: %u, "
                                   "compression saved: %u, method: %s",
                                   XLogRecBlockImageApply(record, block_id) ?
                                   "" : " for WAL verification",
                                   XLogRecGetBlock(record, block_id)->hole_offset,
                                   XLogRecGetBlock(record, block_id)->hole_length,
                                   BLCKSZ -
                                   XLogRecGetBlock(record, block_id)->hole_length -
                                   XLogRecGetBlock(record, block_id)->bimg_len,
                                   method);
                }
                else
                {
                    // Uncompressed image
                    appendStringInfo(buf,
                                   " (FPW%s); hole: offset: %u, length: %u",
                                   XLogRecBlockImageApply(record, block_id) ?
                                   "" : " for WAL verification",
                                   XLogRecGetBlock(record, block_id)->hole_offset,
                                   XLogRecGetBlock(record, block_id)->hole_length);
                }
            }

            if (pretty)
                appendStringInfoChar(buf, '\n');
        }
        else
        {
            // Short format: minimal block information
            if (forknum != MAIN_FORKNUM)
            {
                appendStringInfo(buf,
                               ", blkref #%d: rel %u/%u/%u fork %s blk %u",
                               block_id,
                               rlocator.spcOid, rlocator.dbOid, rlocator.relNumber,
                               forkNames[forknum], blk);
            }
            else
            {
                appendStringInfo(buf,
                               ", blkref #%d: rel %u/%u/%u blk %u",
                               block_id,
                               rlocator.spcOid, rlocator.dbOid, rlocator.relNumber,
                               blk);
            }

            // Handle FPI in short format
            if (XLogRecHasBlockImage(record, block_id))
            {
                if (fpi_len)
                    *fpi_len += XLogRecGetBlock(record, block_id)->bimg_len;

                if (XLogRecBlockImageApply(record, block_id))
                    appendStringInfoString(buf, " FPW");
                else
                    appendStringInfoString(buf, " FPW for WAL verification");
            }
        }
    }

    if (!detailed_format && pretty)
        appendStringInfoChar(buf, '\n');
}
```