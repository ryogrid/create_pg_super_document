# heapgettup_advance_block

## Location
[src/backend/access/heap/heapam.c:798-881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L798-L881)

## Overview
A helper function that calculates the next block number to scan during heap table scanning operations, handling both forward and backward scan directions with proper wraparound logic.

## Definition

```c
static inline BlockNumber
heapgettup_advance_block(HeapScanDesc scan, BlockNumber block, ScanDirection dir)
```
## Detailed Description
This function advances the block number for heap scanning based on the current block and scan direction. For forward scans, it increments the block number and wraps back to block 0 when reaching the end of the heap. For backward scans, it decrements the block number and wraps to the end when reaching block 0. The function handles scan termination conditions, reports scan position for synchronization when allowed, and enforces limits imposed by heap_setscanlimits(). It should only be called for subsequent blocks, not to determine the initial block number.

## Parameters / Member Variables
- : HeapScanDesc - The heap scan descriptor containing scan state and configuration
- : BlockNumber - The current block number being processed
README.md				filter_frequent_symbol_from_csv.py
__pycache__				global_symbols.db
area					import_symbol_reference.py
attnums					output
base.nKeys				process_symbol_definitions.py
contrib					scripts
create_duckdb_index.py			set_file_end_lines.py
data					src
extract_readme_file_header_comments.py	update_symbol_types.py: ScanDirection - Direction of the scan (forward or backward)

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward
  - likely
  - [ss_report_location](../s/ss_report_location.md)
  - SO_ALLOW_SYNC
- Called from (representative examples):
  - [heap_scan_stream_read_next_serial](heap_scan_stream_read_next_serial.md)

## Notes and Other Information
- Assumes non-parallel scans (rs_parallel == NULL)
- For forward scans, reports scan position for synchronization to coordinate with other concurrent scanners
- Handles wraparound at heap boundaries (block 0 to rs_nblocks-1)
- Respects scan limits set by heap_setscanlimits() by decrementing rs_numblocks
- Returns InvalidBlockNumber when the scan should terminate
- Position reporting is only done for forward scans to avoid interfering with other scanners
- The function ensures consistent starting positions across multiple query executions