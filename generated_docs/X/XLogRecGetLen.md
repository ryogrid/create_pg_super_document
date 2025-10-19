# XLogRecGetLen

## Location
[src/backend/access/transam/xlogstats.c:22-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogstats.c#L22-L53)

## Overview
Calculates the size of a WAL record, splitting it into record data size (without full-page images) and full-page image (FPI) data size.

## Definition

```c
void
XLogRecGetLen(XLogReaderState *record, uint32 *rec_len,
			  uint32 *fpi_len)
```
## Detailed Description
This function analyzes a WAL (Write-Ahead Logging) record to determine its size breakdown. It separates the total record size into two components: the actual record data size (excluding full-page images) and the size of full-page image data. This distinction is important for WAL statistics and analysis since FPI data can constitute a significant portion of WAL volume.

The function iterates through all block references in the record, checking each block to see if it contains a full-page image. For blocks that do contain FPI data, it accumulates the image length. The final record length is calculated by subtracting the total FPI length from the total record length.

## Parameters / Member Variables
- `*record`: Pointer to XLogReaderState containing the parsed WAL record
- `*rec_len`: Output parameter to store the record data size (excluding FPI data)
- `*fpi_len`: Output parameter to store the total size of full-page image data
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecMaxBlockId
  - XLogRecHasBlockRef
  - XLogRecHasBlockImage
  - XLogRecGetBlock
  - XLogRecGetTotalLen
- Called from (representative examples):
  - [XLogRecStoreStats](XLogRecStoreStats.md)
  - [XLogDumpDisplayRecord](XLogDumpDisplayRecord.md)

## Notes and Other Information
- The function accesses xlogreader's private decoded backup blocks to get bimg_len
- This is primarily used for WAL statistics collection and pg_waldump functionality
- The separation of FPI and non-FPI data is crucial for understanding WAL overhead

## Simplified Source

```c
void XLogRecGetLen(XLogReaderState *record, uint32 *rec_len, uint32 *fpi_len)
{
    int block_id;

    // Calculate total full-page image (FPI) data size
    *fpi_len = 0;
    for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
        if (!XLogRecHasBlockRef(record, block_id))
            continue;

        // Add FPI length if this block has a full-page image
        if (XLogRecHasBlockImage(record, block_id))
            *fpi_len += XLogRecGetBlock(record, block_id)->bimg_len;
    }

    // Record data length = total length - FPI length
    *rec_len = XLogRecGetTotalLen(record) - *fpi_len;
}
```