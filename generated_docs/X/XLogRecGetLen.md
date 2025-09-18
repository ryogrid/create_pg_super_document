# XLogRecGetLen

## Location
src/backend/access/transam/xlogstats.c: 22 - 53

## Overview
Calculates the size of a WAL record, splitting it into record data size (without full-page images) and full-page image (FPI) data size.

## Definition


## Detailed Description
This function analyzes a WAL (Write-Ahead Logging) record to determine its size breakdown. It separates the total record size into two components: the actual record data size (excluding full-page images) and the size of full-page image data. This distinction is important for WAL statistics and analysis since FPI data can constitute a significant portion of WAL volume.

The function iterates through all block references in the record, checking each block to see if it contains a full-page image. For blocks that do contain FPI data, it accumulates the image length. The final record length is calculated by subtracting the total FPI length from the total record length.

## Parameters / Member Variables
- : Pointer to XLogReaderState containing the parsed WAL record
- : Output parameter to store the record data size (excluding FPI data)
- : Output parameter to store the total size of full-page image data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecMaxBlockId
  - XLogRecHasBlockRef
  - XLogRecHasBlockImage
  - XLogRecGetBlock
  - XLogRecGetTotalLen
- Called from (representative examples):
  - XLogRecStoreStats
  - XLogDumpDisplayRecord

## Notes and Other Information
- The function accesses xlogreader's private decoded backup blocks to get bimg_len
- This is primarily used for WAL statistics collection and pg_waldump functionality
- The separation of FPI and non-FPI data is crucial for understanding WAL overhead