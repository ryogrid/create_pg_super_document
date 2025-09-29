# AppendInvalidationMessageSubGroup

## Location
[src/backend/utils/cache/inval.c:331-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L331-L354)

## Overview
AppendInvalidationMessageSubGroup is a static function that merges one invalidation message subgroup into another by appending the source subgroup's messages to the destination and resetting the source to follow the destination.

## Definition
```c
static void AppendInvalidationMessageSubGroup(InvalidationMsgsGroup *dest, InvalidationMsgsGroup *src, int subgroup)
```

## Detailed Description
This function performs an efficient merge operation between two invalidation message subgroups by updating pointer indices rather than copying message data. It assumes that the messages in both subgroups are stored adjacently in the main message array, allowing for a simple pointer manipulation to merge them. After the merge, the source subgroup is configured to follow the destination subgroup, preventing multiple groups from pointing to the same message array fragment.

The function relies on the invariant that dest->nextmsg[subgroup] equals src->firstmsg[subgroup], meaning the destination's end position matches the source's start position in the shared message array.

## Parameters / Member Variables
- `dest`: Pointer to the destination InvalidationMsgsGroup that will receive the appended messages
- `src`: Pointer to the source InvalidationMsgsGroup whose messages will be appended to the destination
- `subgroup`: Integer identifier specifying which subgroup (CatCacheMsgs or RelCacheMsgs) to operate on

## Dependencies
- Functions called/Symbols referenced:
  - SetSubGroupToFollow (to configure source group to follow destination)
- Data structures used:
  - [InvalidationMsgsGroup](../I/InvalidationMsgsGroup.md)
- Called from:
  - [AppendInvalidationMessages](AppendInvalidationMessages.md) (called twice, once for each subgroup type)

## Notes and Other Information
- This is a static function, only accessible within the inval.c file
- The function performs an assertion check to ensure message adjacency before proceeding
- The merge operation is O(1) as it only updates pointers, not copying actual message data
- Part of PostgreSQL's cache invalidation system that manages message groups efficiently
- The function always calls SetSubGroupToFollow to maintain proper group relationships and prevent pointer aliasing issues

## Simplified Source

```c
static void AppendInvalidationMessageSubGroup(InvalidationMsgsGroup *dest,
                                              InvalidationMsgsGroup *src,
                                              int subgroup)
{
    // Verify messages are adjacent in the main array
    Assert(dest->nextmsg[subgroup] == src->firstmsg[subgroup]);

    // Merge by updating the destination's end pointer
    dest->nextmsg[subgroup] = src->nextmsg[subgroup];

    // Configure source to follow destination to prevent pointer conflicts
    SetSubGroupToFollow(src, dest, subgroup);
}
```