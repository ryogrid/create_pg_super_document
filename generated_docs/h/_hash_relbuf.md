# _hash_relbuf

## Location
[src/backend/access/hash/hashpage.c:266-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L266-L276)

## Overview
This function releases a locked buffer by dropping both its lock and pin (reference count), providing a clean way to release buffer resources in hash index operations.

## Definition

```c
void
_hash_relbuf(Relation rel, Buffer buf)
```
## Detailed Description
 is a wrapper function that releases a hash index buffer by calling . This function serves as the standard way to release buffers in hash index code, ensuring both the lock and the pin (reference count) are properly dropped. The function maintains consistency in buffer management across the hash index access method by providing a uniform interface for buffer release operations.

## Parameters / Member Variables
- : The relation (hash index) associated with the buffer (parameter present for interface consistency but not actively used)
- : The buffer to be released, which must be currently locked and pinned

## Dependencies
- Functions called/Symbols referenced:
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (core buffer management function)

- Called from (representative examples):
  - [hashbulkdelete](hashbulkdelete.md) (bulk deletion operations)
  - [hashbucketcleanup](hashbucketcleanup.md) (bucket cleanup during vacuum)
  - [_hash_doinsert](_hash_doinsert.md) (insertion operations)
  - [_hash_addovflpage](_hash_addovflpage.md) (overflow page management)
  - [_hash_freeovflpage](_hash_freeovflpage.md) (overflow page cleanup)
  - [_hash_squeezebucket](_hash_squeezebucket.md) (bucket reorganization)
  - [_hash_init](_hash_init.md) (index initialization)
  - [_hash_expandtable](_hash_expandtable.md) (table expansion)
  - [_hash_splitbucket](_hash_splitbucket.md) (bucket splitting)
  - [_hash_readnext](_hash_readnext.md)/_hash_readprev (scan operations)

## Notes and Other Information
- This function is a thin wrapper around  but provides interface consistency across hash index operations
- The  parameter is included for API consistency with other hash functions but is not used in the implementation
- Both lock and pin are released atomically, making it safe to use in error recovery paths
- Widely used throughout hash index operations as the standard method for buffer cleanup
- The function does not perform any validation - the caller must ensure the buffer is valid and properly locked before calling