# SlruScanDirCbFindEarliest

## Location
src/backend/access/transam/multixact.c: 3017 - 3039

## Overview
SlruScanDirCbFindEarliest is a callback function used with SlruScanDirectory to find the earliest existing page number in an SLRU (Simple Least Recently Used) directory structure.

## Definition
static bool SlruScanDirCbFindEarliest(SlruCtl ctl, char *filename, int64 segpage, void *data)

## Detailed Description
This function serves as a callback for the SLRU directory scanning mechanism. It examines each SLRU segment file to determine the earliest (lowest) page number that exists in the directory. The function updates a truncation information structure with the earliest page number found during the scan. This is typically used in the context of MultiXact truncation operations to identify the starting point for cleanup operations.

The function uses the SLRU control structure's PagePrecedes function to compare page numbers, ensuring proper ordering semantics for the specific SLRU being scanned.

## Parameters / Member Variables
- `ctl`: SLRU control structure containing metadata and function pointers for the specific SLRU
- `filename`: Name of the current segment file being examined (unused in this callback)
- `segpage`: Page number of the current segment file being processed
- `data`: Pointer to mxtruncinfo structure containing truncation state information

## Dependencies
- Functions called/Symbols referenced:
  - SlruCtl (SLRU control structure)
  - [mxtruncinfo](../m/mxtruncinfo.md) (MultiXact truncation information structure)
- Called from (representative examples):
  - [TruncateMultiXact](../T/TruncateMultiXact.md)

## Notes and Other Information
- Returns false to continue the directory scan (doesn't terminate early)
- Updates the earliestExistingPage field in the truncation info structure
- Part of the MultiXact cleanup and maintenance system
- Uses SLRU-specific page ordering logic via the PagePrecedes function pointer