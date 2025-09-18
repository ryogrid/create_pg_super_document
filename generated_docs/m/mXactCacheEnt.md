# mXactCacheEnt

## Location
src/backend/access/transam/multixact.c: 361 - 367

## Overview
mXactCacheEnt is a cache entry structure that stores MultiXact information in backend-local memory to avoid frequent SLRU access, optimizing multixact lookups within a single transaction.

## Definition
```c
typedef struct mXactCacheEnt
{
    MultiXactId multi;
    int nmembers;
    dlist_node node;
    MultiXactMember members[FLEXIBLE_ARRAY_MEMBER];
} mXactCacheEnt;
```

## Detailed Description
mXactCacheEnt represents a cached MultiXact entry that stores complete multixact information locally in each backend process. This cache mechanism reduces the need to repeatedly access SLRU (Simple Least Recently Used) storage areas when looking up the same MultiXact information multiple times within a transaction.

The cache operates on a per-transaction basis, with entries allocated in a memory context that gets deleted at transaction end. This design assumes that most cached entries will contain the current transaction's own TransactionId and become irrelevant when the next transaction starts. However, the comments indicate this assumption may be flawed for multixacts containing update XIDs that could remain relevant beyond the caching transaction's lifetime.

## Parameters / Member Variables
- `multi`: The MultiXactId that this cache entry represents
- `nmembers`: Number of member transactions in this multixact
- `node`: Doubly-linked list node for organizing cache entries in a list structure
- `members`: Flexible array containing the actual MultiXactMember entries that make up this multixact

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactId (identifier type for multixacts)
  - dlist_node (for doubly-linked list organization)
  - MultiXactMember (member transaction information)
  - FLEXIBLE_ARRAY_MEMBER (for variable-length member array)
- Called from (representative examples):
  - mXactCacheGetBySet (retrieves cache entries by member set)
  - mXactCacheGetById (retrieves cache entries by MultiXactId)
  - mXactCachePut (adds new entries to cache)

## Notes and Other Information
The cache design has acknowledged limitations, particularly around the transaction-scoped lifetime policy which may not be optimal for multixacts containing update XIDs that outlive the original caching transaction. The structure uses a flexible array member to efficiently store variable numbers of multixact members in a single allocation. Cache entries are organized using doubly-linked lists for efficient insertion and removal operations during cache management.