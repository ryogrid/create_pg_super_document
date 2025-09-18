# SlruScanDirCbReportPresence

## Location
[src/backend/access/transam/slru.c:1709-1724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1709-L1724)

## Overview
A callback function used by SlruScanDirectory to report whether any SLRU segment exists that is wholly prior to a specified cutoff page.

## Definition
```c
bool SlruScanDirCbReportPresence(SlruCtl ctl, char *filename, int64 segpage, void *data)
```

## Detailed Description
SlruScanDirCbReportPresence is a specialized callback function designed to work with SlruScanDirectory. Its primary purpose is to determine if there are any SLRU (Simple Least Recently Used) segments that exist before a specified cutoff page. The function takes a cutoff page number passed as data and checks each segment encountered during directory scanning to see if it should be deleted based on the cutoff criteria. When it finds the first segment that meets the deletion criteria, it returns true to stop the scanning process early, making it an efficient presence-checking mechanism.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing SLRU control information and configuration
- `filename`: Name of the SLRU segment file being examined (unused in this callback)
- `segpage`: Page number representing the first page of the segment being examined  
- `data`: Void pointer to the cutoff page number (cast to int64)

## Dependencies
- Functions called/Symbols referenced:
  - [SlruMayDeleteSegment](SlruMayDeleteSegment.md)
- Called from (representative examples):
  - [TruncateCLOG](../T/TruncateCLOG.md)
  - [TruncateCommitTs](../T/TruncateCommitTs.md)

## Notes and Other Information
- Returns true immediately when finding the first segment eligible for deletion, providing early termination optimization
- Part of the SLRU infrastructure used for transaction log management
- The callback follows the standard SlruScanDirectory callback pattern but is optimized for presence detection rather than performing actual operations