# heapam_scan_sample_next_block

## Location
[src/backend/access/heap/heapam_handler.c:2306-2395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L2306-L2395)

## Overview
Advances to the next block in a heap sample scan, supporting both custom sampling methods and sequential scanning with wraparound.

## Definition

```c
static bool
heapam_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
```
## Detailed Description
This function manages block selection and positioning during sample scans of heap relations. It supports two modes of operation: custom sampling methods through the Table Sampling Method (TSM) API, and default sequential scanning with wraparound. The function handles buffer management, scan synchronization for parallel operations, interrupt checking, and prepares pages for tuple extraction. It maintains scan state consistency and ensures proper initialization of the scanning process.

## Parameters / Member Variables
- : The table scan descriptor containing scan state and configuration
- : Sample scan state containing sampling method information and parameters

## Dependencies
- Functions called/Symbols referenced:
  - TsmRoutine.NextSampleBlock (custom sampling method)
  - ReleaseBuffer (buffer management)
  - [ss_report_location](../s/ss_report_location.md) (scan synchronization)
  - CHECK_FOR_INTERRUPTS (interrupt handling)
  - [ReadBufferExtended](../R/ReadBufferExtended.md) (buffer reading)
  - [heap_prepare_pagescan](heap_prepare_pagescan.md) (page preparation for scanning)
  - Various block number and buffer validation functions
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (as part of table access method interface)

## Notes and Other Information
- Supports pluggable sampling methods through the TSM (Table Sampling Method) interface
- Falls back to sequential scanning when no custom sampling method is provided
- Implements wraparound logic to scan the entire relation when doing sequential sampling
- Handles scan synchronization for parallel sample scans (SO_ALLOW_SYNC flag)
- Maintains scan position reporting for coordination with other scan processes
- Includes interrupt checking to allow cancellation of long-running sample operations
- Prepares pages for efficient tuple extraction when in page mode (SO_ALLOW_PAGEMODE)
- Returns false when scan is complete or relation is empty
- Manages buffer strategy for optimal I/O performance during sampling