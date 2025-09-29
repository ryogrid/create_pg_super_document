# smgrunpin

## Location
[src/backend/storage/smgr/smgr.c:265-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L265-L276)

## Overview
Allows an SMgrRelation object to be destroyed at the end of a transaction by decrementing its reference count.

## Definition
```c
void smgrunpin(SMgrRelation reln)
```

## Detailed Description
The `smgrunpin` function decrements the pin count of an SMgrRelation object, potentially allowing it to be destroyed at transaction end. The function first asserts that the relation has at least one pin (pincount > 0), then decrements the count. If the pin count reaches zero, the relation is added back to the unpinned_relns list, making it eligible for cleanup by AtEOXact_SMgr() at transaction end. This function provides the counterpart to smgrpin, completing the reference counting mechanism that controls SMgrRelation object lifetime.

## Parameters / Member Variables
- `reln`: SMgrRelation object to be unpinned

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - SMgrRelation (type)
- Called from (representative examples):
  - [RelationCloseSmgr](../R/RelationCloseSmgr.md) (src/include/utils/rel.h:586)

## Notes and Other Information
- The function includes an assertion to ensure the relation is actually pinned before unpinning
- When pincount reaches 0, the relation is moved to the unpinned_relns list for cleanup
- Relations in the unpinned list are automatically destroyed by AtEOXact_SMgr()
- This function must be called for each corresponding smgrpin call to properly release references
- The object remains valid until AtEOXact_SMgr() is called, even after unpinning

## Simplified Source

```c
void
smgrunpin(SMgrRelation reln)
{
    // Ensure relation is actually pinned
    Assert(reln->pincount > 0);

    // Decrement reference count
    reln->pincount--;

    // If no more references, add to cleanup list
    if (reln->pincount == 0)
        dlist_push_tail(&unpinned_relns, &reln->node);
}
```