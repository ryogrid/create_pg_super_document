# ginFreeScanKeys

## Location
src/backend/access/gin/ginscan.c: 238 - 267

## Overview
Releases and cleans up all current scan keys and associated resources in a GIN index scan, preparing for scan termination or restart.

## Definition


## Detailed Description
The  function performs comprehensive cleanup of all scan-related resources in a GIN index scan. It iterates through all scan entries and releases buffers, frees item pointer lists, ends TID bitmap iterations, and frees TID bitmaps. After cleaning up individual entries, it resets the key memory context and clears all scan key references.

This function is essential for proper resource management in GIN scans, ensuring that buffers are released back to the buffer pool, memory is freed, and no resources are leaked when a scan is completed or restarted.

## Parameters
- : GIN scan opaque data structure containing all scan state and resources

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseBuffer
  - pfree
  - tbm_end_iterate
  - tbm_free
  - MemoryContextReset
- Called from:
  - gingetbitmap
  - ginrescan
  - ginendscan

## Notes and Other Information
- Safely handles NULL keys by returning early if no keys exist
- Systematically releases all types of resources: buffers, lists, iterators, and bitmaps
- Resets the key memory context to free all key-related allocations at once
- Clears all scan state pointers to prevent dangling references
- Essential for proper resource cleanup in GIN index access method
- Called during scan termination, restart, and error recovery scenarios