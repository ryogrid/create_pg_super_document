# _hash_getbuf

## Location
[src/backend/access/hash/hashpage.c:70-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L70-L95)

## Overview
Gets a buffer for a specific block number in a hash index for read or write operations, ensuring the page is valid and properly locked.

## Definition


## Detailed Description
This function retrieves a buffer for an existing page in a hash index by its block number. It is specifically designed to access pages that are already known to exist and be valid - it cannot be used to extend the index (P_NEW is explicitly disallowed). The function performs several critical operations:

1. Validates that the block number is not P_NEW (new page allocation)
2. Reads the buffer from disk using the standard buffer manager
3. Applies the appropriate lock based on the access parameter
4. Validates the page contents using _hash_checkpage with the provided flags

The returned buffer is both "locked and pinned" - meaning it has an incremented reference count and appropriate lock held.

## Parameters / Member Variables
- : The hash index relation to read from
- : Block number of the page to retrieve (must not be P_NEW)  
- : Lock mode - HASH_READ, HASH_WRITE, or HASH_NOLOCK
- : Bitwise OR of allowed page types for validation by _hash_checkpage

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md) (buffer manager function to read a page)
  - [LockBuffer](../L/LockBuffer.md) (applies lock to buffer)
  - _hash_checkpage (validates page contents and type)
  - P_NEW, HASH_NOLOCK (constants)
- Called from (representative examples):
  - [_hash_doinsert](_hash_doinsert.md) (during tuple insertion)
  - [_hash_next](_hash_next.md)/_hash_first (during index scans)
  - [_hash_splitbucket](_hash_splitbucket.md) (during bucket splitting)
  - [_hash_addovflpage](_hash_addovflpage.md)/_hash_freeovflpage (overflow page management)

## Notes and Other Information
- This function explicitly prevents extending the hash index - use _hash_getnewbuf for that purpose
- The buffer is returned in a "locked and pinned" state requiring proper cleanup
- Page validation is always performed to ensure data integrity
- Commonly used throughout hash index operations for accessing existing pages