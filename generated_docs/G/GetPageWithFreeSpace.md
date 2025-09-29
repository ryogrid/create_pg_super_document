# GetPageWithFreeSpace

## Location
[src/backend/storage/freespace/freespace.c:137-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L137-L153)

## Overview
GetPageWithFreeSpace is a core Free Space Map (FSM) API function that searches for a page in a relation with at least the specified amount of free space available for new tuples.

## Definition

```c
BlockNumber
GetPageWithFreeSpace(Relation rel, Size spaceNeeded)
```
## Detailed Description
This function serves as the primary entry point for finding pages with sufficient free space in PostgreSQL's Free Space Map system. It converts the requested space amount into a space category using the FSM's categorization scheme, then searches the FSM for a suitable page. The function is designed to be fault-tolerant - callers must be prepared for the possibility that the returned page may have insufficient space by the time they acquire a lock on it, due to concurrent modifications.

The function can trigger FSM updates if it encounters FSM entries pointing to blocks beyond the end of the relation, helping maintain FSM consistency. If no suitable page is found, callers should extend the relation to create new pages.

## Parameters / Member Variables
- : The relation (table/index) to search for free space
- : The minimum amount of free space required in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [fsm_space_needed_to_cat](../f/fsm_space_needed_to_cat.md)
  - [fsm_search](../f/fsm_search.md)
- Called from (representative examples):
  - [brin_getinsertbuffer](../b/brin_getinsertbuffer.md)
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md)
  - [GetFreeIndexPage](GetFreeIndexPage.md)

## Notes and Other Information
- Returns InvalidBlockNumber when no suitable page is found
- Callers should use RecordAndGetPageWithFreeSpace when they discover the returned page has insufficient space
- Part of PostgreSQL's Free Space Map public API
- Thread-safe and handles concurrent access scenarios
- Located in src/backend/storage/freespace/freespace.c:137-153

## Simplified Source
```c
BlockNumber GetPageWithFreeSpace(Relation rel, Size spaceNeeded) {
    // Convert space requirement to FSM category
    uint8 min_cat = fsm_space_needed_to_cat(spaceNeeded);

    // Search FSM for a page with sufficient free space
    return fsm_search(rel, min_cat);
}
```