# xl_heap_update

## Location
[src/include/access/heapam_xlog.h:217-230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam_xlog.h#L217-L230)

## Overview
A WAL record structure that captures the essential information needed to replay heap tuple update operations during crash recovery and replication.

## Definition
```c
typedef struct xl_heap_update
{
    TransactionId old_xmax;      /* xmax of the old tuple */
    OffsetNumber old_offnum;     /* old tuple's offset */
    uint8        old_infobits_set; /* infomask bits to set on old tuple */
    uint8        flags;
    TransactionId new_xmax;      /* xmax of the new tuple */
    OffsetNumber new_offnum;     /* new tuple's offset */

    /*
     * If XLH_UPDATE_CONTAINS_OLD_TUPLE or XLH_UPDATE_CONTAINS_OLD_KEY flags
     * are set, xl_heap_header and tuple data for the old tuple follow.
     */
} xl_heap_update;
```

## Detailed Description
The xl_heap_update structure is a critical component of PostgreSQL's Write-Ahead Logging system for tuple update operations. It captures the minimum information necessary to replay an update operation during crash recovery or streaming replication. This includes both HOT (Heap-Only Tuple) updates and regular updates that may span multiple pages.

The structure contains transaction visibility information, tuple locations for both old and new versions, and various flags that control how the update should be replayed. The actual tuple data may follow this structure depending on the flags set, allowing for flexible WAL record formats that optimize space usage.

The update operation may involve prefix/suffix compression where common parts of the old and new tuples are not duplicated in the WAL record, and may include full-page images depending on the specific update scenario and database configuration.

## Parameters / Member Variables
- `old_xmax`: Transaction ID that will be set as the xmax (deleting transaction) of the old tuple version
- `old_offnum`: Offset number (slot) of the old tuple within its page
- `old_infobits_set`: Bitmask of infomask bits that need to be set on the old tuple during replay
- `flags`: Control flags indicating what additional data follows and how the update should be processed
- `new_xmax`: Transaction ID for the xmax field of the new tuple version  
- `new_offnum`: Offset number (slot) where the new tuple version is placed

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (transaction identifier type)
  - OffsetNumber (tuple offset type)
- Called from (representative examples):
  - [log_heap_update](../l/log_heap_update.md) (src/backend/access/heap/heapam.c:8821)
  - [heap_xlog_update](../h/heap_xlog_update.md) (src/backend/access/heap/heapam.c:9861)
  - [heap_desc](../h/heap_desc.md) (src/backend/access/rmgrdesc/heapdesc.c:208, 218)
  - [DecodeUpdate](../D/DecodeUpdate.md) (src/backend/replication/logical/decode.c:968, 973)
  - SizeOfHeapUpdate (src/include/access/heapam_xlog.h:232)

## Notes and Other Information
- The flags field can contain combinations of XLH_UPDATE_* constants like XLH_UPDATE_CONTAINS_OLD_TUPLE, XLH_UPDATE_CONTAINS_NEW_TUPLE, XLH_UPDATE_PREFIX_FROM_OLD, etc.
- Supports both regular updates and HOT updates where the new tuple is placed on the same page
- May include backup blocks for both old and new pages depending on the update scenario
- The structure size is calculated by SizeOfHeapUpdate macro, which extends to the new_offnum field
- [Variable](../V/Variable.md)-length data (tuple headers and data) may follow this fixed structure based on the flags
- Critical for maintaining ACID properties through WAL-based crash recovery and streaming replication