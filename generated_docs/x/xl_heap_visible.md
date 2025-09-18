# xl_heap_visible

## Location
src/include/access/heapam_xlog.h: 438 - 442

## Overview
A WAL (Write-Ahead Logging) record structure that contains information needed for setting visibility map bits during heap operations.

## Definition


## Detailed Description
The xl_heap_visible structure is used in PostgreSQL's Write-Ahead Logging system to record information about visibility map bit operations. This structure is logged when the visibility map is updated to mark pages as all-visible or all-frozen. The visibility map is a critical component of PostgreSQL's MVCC (Multi-Version Concurrency Control) system and vacuum optimization.

The structure contains the minimum information necessary to replay visibility map updates during recovery. It works in conjunction with backup blocks for the visibility map buffer and heap buffer to ensure consistent recovery.

## Parameters / Member Variables
- : TransactionId that represents the conflict horizon for snapshot isolation - transactions with snapshots older than this horizon may conflict with the visibility change
- : Bit flags indicating the type of visibility operation being performed (e.g., setting all-visible, all-frozen bits)

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (type)
  - uint8 (type)
- Called from (representative examples):
  - log_heap_visible (src/backend/access/heap/heapam.c:8785)
  - heap_xlog_visible (src/backend/access/heap/heapam.c:9366)
  - heap2_desc (src/backend/access/rmgrdesc/heapdesc.c:340)
  - SizeOfHeapVisible (src/include/access/heapam_xlog.h:444)

## Notes and Other Information
- Used with backup block 0 (visibility map buffer) and backup block 1 (heap buffer) for complete recovery information
- Part of the heap WAL record types for crash recovery and replication
- The structure is designed to be minimal to reduce WAL overhead while providing sufficient information for recovery
- Critical for maintaining visibility map consistency across database crashes and restarts