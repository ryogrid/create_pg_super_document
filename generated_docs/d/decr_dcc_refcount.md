# decr_dcc_refcount

## Location
[src/backend/utils/cache/typcache.c:1243-1253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1243-L1253)

## Overview
A reference counting function that decrements a DomainConstraintCache's reference count and frees the associated memory context when no references remain.

## Definition
```c
static void decr_dcc_refcount(DomainConstraintCache *dcc)
```

## Detailed Description
This function implements reference counting for DomainConstraintCache objects in PostgreSQL's type cache system. It safely decrements the reference count of a domain constraint cache and performs automatic cleanup when the reference count reaches zero.

The function:
1. Asserts that the reference count is positive (debugging safety check)
2. Decrements the reference count atomically
3. If the reference count reaches zero or below, deletes the entire memory context associated with the cache

This approach ensures that shared DomainConstraintCache objects are properly cleaned up when no longer needed, preventing memory leaks while allowing multiple references to the same constraint data.

## Parameters / Member Variables
- `dcc`: A pointer to the DomainConstraintCache whose reference count should be decremented. The cache must have a positive reference count when this function is called.

## Dependencies
- Functions called/Symbols referenced:
  - [DomainConstraintCache](../D/DomainConstraintCache.md) (struct type)
  - Assert (debugging macro)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [load_domaintype_info](../l/load_domaintype_info.md)
  - [dccref_deletion_callback](dccref_deletion_callback.md)
  - [UpdateDomainConstraintRef](../U/UpdateDomainConstraintRef.md)

## Notes and Other Information
- This is a static function, only accessible within typcache.c
- Part of PostgreSQL's memory management system for domain constraint caches
- Uses Assert() for debugging - the assertion will be compiled out in non-debug builds
- Deleting the memory context automatically frees all memory allocated within it, including the DomainConstraintCache structure itself and all associated constraint data
- Critical for preventing memory leaks in long-running PostgreSQL sessions
- Works in conjunction with reference counting to enable safe sharing of constraint cache data across multiple type cache entries