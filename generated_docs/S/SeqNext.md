# SeqNext

## Location
src/backend/executor/nodeSeqscan.c: 50 - 88

## Overview
SeqNext is a static function that serves as the core workhorse for ExecSeqScan, responsible for retrieving the next tuple from a sequential table scan.

## Definition


## Detailed Description
SeqNext implements the fundamental tuple retrieval logic for sequential scans in PostgreSQL. It manages the table scan descriptor lifecycle, handles both parallel and non-parallel scan scenarios, and retrieves tuples from the underlying storage engine. The function first checks if a scan descriptor exists, and if not (which occurs for non-parallel scans or serial execution of planned parallel scans), it initializes one using table_beginscan. It then calls table_scan_getnextslot to fetch the next tuple in the specified scan direction.

## Parameters / Member Variables
- `node`: SeqScanState pointer containing the scan state information, including the current relation, scan descriptor, and tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - table_beginscan
  - table_scan_getnextslot
  - SeqScanState
  - TableScanDesc
  - ScanDirection
- Called from (representative examples):
  - ExecSeqScan

## Notes and Other Information
- This is a static function, only accessible within nodeSeqscan.c
- Handles the distinction between parallel and non-parallel scan execution
- Returns NULL when no more tuples are available
- The function manages scan descriptor initialization lazily for non-parallel scans