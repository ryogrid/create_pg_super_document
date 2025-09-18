# XLogRecStoreStats

## Location
src/backend/access/transam/xlogstats.c: 54 - 96

## Overview
Stores per-resource manager and per-record type statistics for a given WAL record into an XLogStats structure.

## Definition


## Detailed Description
This function updates WAL statistics by analyzing a parsed WAL record and incrementing counters and size accumulators in the provided XLogStats structure. It maintains both per-resource manager statistics and detailed per-record type statistics.

The function first extracts the resource manager ID from the record and calls XLogRecGetLen to separate record data from full-page image data. It then updates global counts and per-rmgr statistics including record count, record length, and FPI length.

For detailed per-record statistics, it extracts the record type identifier from the xl_info field (upper 4 bits), with special handling for XACT records which use a different bit layout. The statistics are stored in a two-dimensional array indexed by [rmgr_id][record_type].

## Parameters / Member Variables
- : Pointer to XLogStats structure to update with the record's statistics  
- : Pointer to XLogReaderState containing the parsed WAL record to analyze

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetRmid
  - [XLogRecGetLen](XLogRecGetLen.md)
  - XLogRecGetInfo
- Types referenced:
  - [XLogStats](XLogStats.md)
  - RmgrId
- Called from (representative examples):
  - [main](../m/main.md) (in pg_waldump)

## Notes and Other Information
- XACT records require special handling due to their xl_info bit layout (uses lower 3 bits for opcode)
- The record type identifier uses the upper 4 bits of xl_info, allowing 16 possible record types per resource manager
- This function is primarily used by pg_waldump for generating detailed WAL analysis reports
- Statistics include both count and size metrics for comprehensive WAL analysis