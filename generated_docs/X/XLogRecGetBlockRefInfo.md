# XLogRecGetBlockRefInfo

## Location
src/backend/access/rmgrdesc/xlogdesc.c: 231 - 353

## Overview
Generates formatted information about all block references contained in a WAL (Write-Ahead Log) record, including details about full-page images and compression.

## Definition


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
  - appendStringInfo, appendStringInfoChar, appendStringInfoString (string buffer operations)
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