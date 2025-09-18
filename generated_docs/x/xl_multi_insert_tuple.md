# xl_multi_insert_tuple

## Location
src/include/access/heapam_xlog.h: 189 - 196

## Overview
A structure that represents the metadata and header information for a single tuple within a multi-insert WAL (Write-Ahead Log) record, containing essential tuple formatting information for WAL replay.

## Definition


## Detailed Description
The xl_multi_insert_tuple structure serves as a compact header for individual tuples within a multi-insert operation that is being logged to the WAL. This structure is part of PostgreSQL's Write-Ahead Logging system for bulk insert operations, where multiple tuples are inserted in a single transaction and need to be efficiently logged for crash recovery and replication purposes.

The structure contains the essential metadata needed to reconstruct a tuple during WAL replay, including the tuple's data length and the critical heap tuple header flags (t_infomask and t_infomask2) that describe the tuple's properties such as null values, variable-length attributes, and transaction visibility information. The actual tuple data immediately follows this header structure in memory.

## Parameters / Member Variables
- : Size in bytes of the tuple data that immediately follows this structure
- : Second tuple header bitmask containing flags for number of attributes and HOT (Heap-Only Tuple) information
- : Primary tuple header bitmask containing flags for null values, variable-length attributes, transaction status, and other tuple properties
- : Tuple header offset indicating where the actual tuple data begins within the tuple structure

## Dependencies
- Functions called/Symbols referenced:
  - (This is a data structure with no direct function calls)
- Called from (representative examples):
  - heap_multi_insert (src/backend/access/heap/heapam.c:2530, 2536)
  - heap_xlog_multi_insert (src/backend/access/heap/heapam.c:9782, 9796)
  - DecodeMultiInsert (src/backend/replication/logical/decode.c:1163, 1174)
  - SizeOfMultiInsertTuple (src/include/access/heapam_xlog.h:198)

## Notes and Other Information
- This structure is designed for space efficiency in WAL records, containing only the minimum information needed to reconstruct tuples during recovery
- The actual tuple data follows immediately after this structure in memory, making it a variable-length record
- Used specifically for multi-insert operations where multiple tuples are logged together to reduce WAL volume
- The SizeOfMultiInsertTuple macro calculates the fixed size of this structure (excluding the variable tuple data)
- Part of the heap access method's WAL logging infrastructure for bulk operations