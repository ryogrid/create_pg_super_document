# xl_heap_lock

## Location
[src/include/access/heapam_xlog.h:395-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam_xlog.h#L395-L401)

## Overview
A WAL record structure used to log tuple locking operations in PostgreSQL heap tables, containing essential information about the locked tuple and lock characteristics.

## Definition
```c
typedef struct xl_heap_lock
{
    TransactionId xmax;         /* might be a MultiXactId */
    OffsetNumber  offnum;       /* locked tuple's offset on page */
    uint8         infobits_set; /* infomask and infomask2 bits to set */
    uint8         flags;        /* XLH_LOCK_* flag bits */
} xl_heap_lock;
```

## Detailed Description
The `xl_heap_lock` structure is a WAL record format used to log tuple locking operations in PostgreSQL's heap access method. When a tuple is locked (either for SELECT FOR UPDATE, SELECT FOR SHARE, or similar operations), this structure captures the essential information needed to replay the lock operation during crash recovery or replication.

This record is written to the WAL whenever a tuple lock needs to be logged, ensuring that lock information can be properly restored during recovery scenarios. The structure contains all necessary details to recreate the lock state on the target tuple.

## Parameters / Member Variables
- `xmax`: The transaction ID that owns the lock, which might actually be a MultiXactId when multiple transactions are involved
- `offnum`: The offset number of the locked tuple within its heap page
- `infobits_set`: Bitmask indicating which infomask and infomask2 bits should be set on the tuple header
- `flags`: Additional flag bits using XLH_LOCK_* constants to specify lock characteristics

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [heap_update](../h/heap_update.md)
  - [heap_lock_tuple](../h/heap_lock_tuple.md)  
  - [heap_xlog_lock](../h/heap_xlog_lock.md)
  - [heap_desc](../h/heap_desc.md)
  - SizeOfHeapLock

## Notes and Other Information
- Part of PostgreSQL's Write-Ahead Logging system for maintaining data consistency
- Used in both normal operation logging and WAL replay during recovery
- The xmax field can represent either a single TransactionId or a MultiXactId for shared locks
- Works in conjunction with tuple header infomask bits to represent various lock modes
- Essential for crash recovery and streaming replication to maintain lock semantics