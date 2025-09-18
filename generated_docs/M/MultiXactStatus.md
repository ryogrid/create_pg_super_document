# MultiXactStatus

## Location
src/include/access/multixact.h: 47 - 48

## Overview
MultiXactStatus is an enum that defines the possible lock modes for multi-transaction operations in PostgreSQL, representing different types of tuple locks and update operations.

## Definition


## Detailed Description
MultiXactStatus represents the different lock modes that can be held by transactions participating in a multi-transaction (multixact). These status values correspond to PostgreSQL's various locking modes used for tuple locks and updates. The first four modes (0x00-0x03) are for explicit tuple locks acquired through SELECT FOR statements, while the last two modes (0x04-0x05) are for actual update and delete operations.

The enum values are carefully ordered and designed to work with PostgreSQL's lock conflict detection and resolution mechanisms. The ISUPDATE_from_mxstatus macro uses the fact that update operations have status values greater than MultiXactStatusForUpdate to distinguish between lock-only and update operations.

## Parameters / Member Variables
-  (0x00): FOR KEY SHARE lock mode - allows concurrent key shares and updates to non-key columns
-  (0x01): FOR SHARE lock mode - allows concurrent shares but prevents updates
-  (0x02): FOR NO KEY UPDATE lock mode - prevents updates but allows key shares
-  (0x03): FOR UPDATE lock mode - exclusive lock preventing all concurrent access
-  (0x04): Actual update operation that doesn't modify key columns
-  (0x05): Actual update operation that may modify key columns, or delete operation

## Dependencies
- Functions called/Symbols referenced:
  - None (enum definition)
- Called from (representative examples):
  - heap_update (src/backend/access/heap/heapam.c:3220)
  - heap_lock_tuple (src/backend/access/heap/heapam.c:4919)
  - compute_new_xmax_infomask (src/backend/access/heap/heapam.c:5341)
  - MultiXactIdCreate (src/backend/access/transam/multixact.c:433)
  - mxstatus_to_string (src/backend/access/transam/multixact.c:1746)
  - MultiXactMember struct (src/include/access/multixact.h:59)

## Notes and Other Information
- The enum values are used in the MultiXactMember structure to track what type of lock each transaction holds
- MaxMultiXactStatus is defined as MultiXactStatusUpdate for validation purposes
- The ISUPDATE_from_mxstatus(status) macro returns true for status values > MultiXactStatusForUpdate, distinguishing actual updates from lock-only operations
- String representations are provided by mxstatus_to_string(): "keysh", "sh", "fornokeyupd", "forupd", "nokeyupd", "upd"
- These status values are critical for PostgreSQL's concurrency control and deadlock detection mechanisms