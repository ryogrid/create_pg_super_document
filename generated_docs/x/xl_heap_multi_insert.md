# xl_heap_multi_insert

## Location
[src/include/access/heapam_xlog.h:180-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam_xlog.h#L180-L185)

## Overview
The xl_heap_multi_insert struct represents the WAL record data for bulk heap tuple insertion operations, enabling efficient logging of multiple tuple insertions in a single WAL record.

## Definition

```c
typedef struct xl_heap_multi_insert
{
	uint8		flags;
	uint16		ntuples;
	OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} xl_heap_multi_insert;
```
## Detailed Description
This structure is used to record bulk insertion operations where multiple tuples are inserted into a heap table simultaneously. It provides an efficient WAL representation for operations like COPY, INSERT with multiple VALUES clauses, or other bulk loading scenarios. The structure uses a flexible array member to store the offset positions of all inserted tuples, while the actual tuple data and headers are stored separately in the WAL record's data blocks. This design optimizes both WAL space usage and recovery performance for bulk operations.

## Parameters / Member Variables
- : Control flags indicating special conditions such as whether the page was reinitialized (XLOG_HEAP_INIT_PAGE)
- : The number of tuples being inserted in this multi-insert operation
- : A flexible array containing the offset numbers where each tuple was inserted within the page (omitted if the entire page is reinitialized)

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - [heap_multi_insert](../h/heap_multi_insert.md) (creates WAL records for bulk insertions)
  - [heap_xlog_multi_insert](../h/heap_xlog_multi_insert.md) (replays multi-insert operations during recovery)
  - [heap2_desc](../h/heap2_desc.md) (describes multi-insert WAL records for debugging)
  - [DecodeMultiInsert](../D/DecodeMultiInsert.md) (logical replication decoding of bulk insert operations)

## Notes and Other Information
- Uses FLEXIBLE_ARRAY_MEMBER for efficient storage of variable numbers of offset positions
- The SizeOfHeapMultiInsert macro calculates the actual size including the variable-length offsets array
- Actual tuple data is stored in block 0 as xl_multi_insert_tuple structures with proper alignment padding
- The offsets array is omitted when XLOG_HEAP_INIT_PAGE flag is set (entire page reinitialization)
- Significantly more efficient than logging individual insert operations for bulk scenarios
- Critical for performance of bulk loading operations and their recovery/replication
- Each tuple in the data block has its own xl_multi_insert_tuple header followed by tuple data