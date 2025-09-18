# _hash_dropbuf

## Location
src/backend/access/hash/hashpage.c: 277 - 288

## Overview
This function releases an unlocked buffer by dropping only its pin (reference count), specifically designed for buffers on which no lock is currently held.

## Definition


## Detailed Description
 is a specialized buffer release function that drops the pin (reference count) on a buffer without attempting to release any locks. This function is specifically designed for situations where the buffer is not locked but still has a reference count that needs to be decremented. It serves as a counterpart to , which releases both locks and pins, making it suitable for different buffer management scenarios in hash index operations.

## Parameters / Member Variables
- : The relation (hash index) associated with the buffer (parameter present for interface consistency but not actively used)
- : The buffer to be released, which must be currently pinned but not locked

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseBuffer (core buffer management function for unpinning)

- Called from (representative examples):
  - hashbulkdelete (bulk deletion operations)
  - _hash_doinsert (insertion operations)
  - _hash_dropscanbuf (scan buffer cleanup)
  - _hash_expandtable (table expansion operations)
  - _hash_finish_split (split completion)
  - _hash_getbucketbuf_from_hashkey (bucket buffer management)
  - _hash_next/_hash_readprev/_hash_first (scan navigation)

## Notes and Other Information
- Unlike , this function only drops the pin and does not attempt to release any locks
- The  parameter is included for API consistency with other hash functions but is not used in the implementation
- Primarily used in scenarios where buffers are accessed without exclusive locking or where locks have been released separately
- Essential for proper reference counting in hash index scan operations where buffers may be held temporarily without locks
- The caller must ensure the buffer is not locked before calling this function, as it does not handle lock release
- Used extensively in hash index expansion and scan operations where buffer access patterns differ from standard locked operations