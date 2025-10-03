# _hash_getbuf_with_strategy

## Location
[src/backend/access/hash/hashpage.c:239-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L239-L265)

## Overview
This function retrieves a buffer for a hash index page with a specified buffer access strategy, providing a variant of the standard buffer acquisition function that allows customization of memory management during operations like VACUUM.

## Definition

```c
Buffer
_hash_getbuf_with_strategy(Relation rel, BlockNumber blkno,
						   int access, int flags,
						   BufferAccessStrategy bstrategy)
```
## Detailed Description
 is a specialized version of the standard  function that allows specification of a custom buffer access strategy. This function is primarily used during VACUUM operations where different memory management policies may be beneficial for performance. The function validates that the block number is not P_NEW (which is not supported in hash indexes), reads the buffer using the specified strategy, applies the requested lock, and validates the page structure.

## Parameters / Member Variables
- : The relation (hash index) from which to retrieve the buffer
- : The block number of the page to retrieve (must not be P_NEW)
- : The type of lock to acquire on the buffer (or HASH_NOLOCK for no locking)
- : Validation flags passed to  for page structure checking
- : The buffer access strategy to use for memory management policy

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBufferExtended](../R/ReadBufferExtended.md) (buffer management)
  - [LockBuffer](../L/LockBuffer.md) (buffer locking)
  - [_hash_checkpage](_hash_checkpage.md) (page validation)
  - MAIN_FORKNUM (fork number constant)
  - RBM_NORMAL (read buffer mode)
  - HASH_NOLOCK (no-lock constant)
  - P_NEW (new page constant - explicitly rejected)

- Called from (representative examples):
  - [hashbucketcleanup](hashbucketcleanup.md) (vacuum cleanup operations)
  - [_hash_freeovflpage](_hash_freeovflpage.md) (overflow page management)
  - [_hash_squeezebucket](_hash_squeezebucket.md) (bucket reorganization)

## Notes and Other Information
- This function explicitly rejects P_NEW block numbers with an ERROR, as hash indexes do not support dynamic page creation through this mechanism
- The function is primarily used in maintenance operations (VACUUM) where custom buffer strategies can improve memory utilization
- Unlike regular buffer acquisition, this function allows fine-grained control over buffer replacement policies through the BufferAccessStrategy parameter
- The function maintains the same locking and validation semantics as the standard  function

## Simplified Source

```c
Buffer _hash_getbuf_with_strategy(Relation rel, BlockNumber blkno,
                                 int access, int flags,
                                 BufferAccessStrategy bstrategy) {
    // P_NEW not allowed - this function only accesses existing pages
    if (blkno == P_NEW) {
        elog(ERROR, "hash AM does not use P_NEW");
    }

    // Read buffer using the specified access strategy
    Buffer buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
                                   RBM_NORMAL, bstrategy);

    // Apply requested lock (unless HASH_NOLOCK)
    if (access != HASH_NOLOCK) {
        LockBuffer(buf, access);
    }

    // Validate page contents and type
    _hash_checkpage(rel, buf, flags);

    return buf;  // Buffer is now locked, pinned, and validated
}
```