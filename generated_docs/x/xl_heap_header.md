# xl_heap_header

## Location
[src/include/access/heapam_xlog.h:149-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam_xlog.h#L149-L154)

## Overview
The xl_heap_header struct stores the essential tuple header fields that must be preserved in WAL records for heap tuple operations, providing a compact representation of tuple metadata.

## Definition

```c
typedef struct xl_heap_header
{
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
} xl_heap_header;
```
## Detailed Description
This structure represents a compressed version of the heap tuple header that gets stored in WAL records for insert and update operations. Rather than storing the complete HeapTupleHeaderData structure, PostgreSQL optimizes WAL space by only storing the fields that cannot be reconstructed from other information available during WAL replay. The three fields stored are critical for properly reconstructing the tuple's structure and visibility information during recovery.

## Parameters / Member Variables
- `t_infomask2`: Secondary information mask containing attribute count and HOT (Heap-Only Tuple) update flags
- `t_infomask`: Primary information mask containing visibility, null bitmap, and storage format flags
- `t_hoff`: Tuple header offset, indicating where the actual tuple data begins after the header and null bitmap
## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md) (records tuple header in WAL)
  - [heap_delete](../h/heap_delete.md) (preserves header information)
  - [log_heap_update](../l/log_heap_update.md) (stores both old and new tuple headers)
  - [heap_xlog_insert](../h/heap_xlog_insert.md) (reconstructs tuple during WAL replay)
  - [heap_xlog_update](../h/heap_xlog_update.md) (reconstructs updated tuple during replay)
  - [DecodeXLogTuple](../D/DecodeXLogTuple.md) (logical replication tuple decoding)

## Notes and Other Information
- Optimizes WAL space by storing only essential, non-reconstructible header fields
- The SizeOfHeapHeader macro provides the size of this structure
- Other HeapTupleHeaderData fields like transaction IDs are reconstructed from WAL context
- Critical for maintaining proper tuple structure and MVCC visibility during crash recovery
- Used in conjunction with tuple data to fully reconstruct heap tuples during WAL replay
- The t_hoff field is particularly important for variable-length tuple headers with null bitmaps