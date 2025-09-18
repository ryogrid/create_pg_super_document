# heapgettup_initial_block

## Location
src/backend/access/heap/heapam.c: 674 - 720

## Overview
heapgettup_initial_block determines the starting block number for a heap scan based on scan direction, handling both forward and backward scans with proper consideration for scan limits and synchronization.

## Definition


## Detailed Description
This function calculates the appropriate initial block number for heap scanning operations, taking into account scan direction and various scan parameters. For forward scans, it simply returns the configured start block. For backward scans, it performs more complex logic: it disables synchronized scanning (since backward scans are rare and would interfere with forward scanners), calculates the ending block considering any scan limits set by heap_setscanlimits(), and handles wraparound cases when the scan doesn't start from block 0. The function returns InvalidBlockNumber for empty tables or when no blocks are available to scan.

## Parameters / Member Variables
- `scan`: HeapScanDesc containing scan configuration including block counts and start position
- `dir`: ScanDirection indicating forward or backward scanning

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward
  - SO_ALLOW_SYNC (flag manipulation)
- Called from (representative examples):
  - [heap_scan_stream_read_next_serial](heap_scan_stream_read_next_serial.md)

## Notes and Other Information
- Marked as pg_noinline since it's only called during scan initialization
- Only works with serial scans (rs_parallel must be NULL)
- Asserts that scan is not yet initialized (rs_inited must be false)
- Disables synchronized scanning for backward scans to avoid interference
- Handles scan limit adjustments made by heap_setscanlimits()
- Returns InvalidBlockNumber for empty tables or when no blocks are available
- Implements proper wraparound logic for backward scans that don't start from block 0
- Critical component of heap scan initialization that determines the scan's starting point