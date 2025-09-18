# xl_heap_lock_updated

## Location
src/include/access/heapam_xlog.h: 406 - 412

## Overview
A WAL record structure specifically used to log locking operations on updated versions of tuples in PostgreSQL heap tables, tracking lock information for tuple update chains.

## Definition
```c
typedef struct xl_heap_lock_updated
{
    TransactionId xmax;
    OffsetNumber  offnum;
    uint8         infobits_set;
    uint8         flags;
} xl_heap_lock_updated;
```

## Detailed Description
The `xl_heap_lock_updated` structure is a specialized WAL record format used to log locking operations on updated versions of tuples in PostgreSQL's heap access method. This structure is specifically designed to handle the complexities of locking tuples that are part of an update chain, where a tuple has been updated and both the old and new versions may need to be locked.

This record type is crucial for maintaining proper locking semantics in MVCC (Multi-Version Concurrency Control) scenarios where tuple updates create chains of related tuples. The structure ensures that lock information for updated tuple versions can be properly restored during crash recovery or replicated to standby servers.

## Parameters / Member Variables
- `xmax`: The transaction ID that owns the lock on the updated tuple version
- `offnum`: The offset number of the locked tuple within its heap page
- `infobits_set`: Bitmask indicating which infomask and infomask2 bits should be set on the tuple header
- `flags`: Additional flag bits to specify lock characteristics and behavior

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - heap_lock_updated_tuple_rec
  - heap_xlog_lock_updated
  - heap2_desc
  - SizeOfHeapLockUpdated

## Notes and Other Information
- Part of PostgreSQL's Write-Ahead Logging system for tuple update chain locking
- Distinct from `xl_heap_lock` to handle specific requirements of updated tuple versions
- Essential for maintaining lock consistency across tuple update chains during recovery
- Used in conjunction with MVCC mechanisms to ensure proper concurrent access control
- Critical for scenarios involving SELECT FOR UPDATE/SHARE operations on updated tuples