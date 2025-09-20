# ScanSourceDatabasePgClassPage

## Location
[src/backend/commands/dbcommands.c:328-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L328-L390)

## Overview
ScanSourceDatabasePgClassPage processes a single page of the source database's pg_class relation to extract visible tuples and build a list of relations that need to be copied during database creation.

## Definition

```c
structure. */
		tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
```
## Detailed Description
This function performs low-level page scanning of a pg_class page, iterating through all line pointers (ItemIds) on the page to identify valid, visible heap tuples. For each tuple found, it:

1. Gets the maximum offset number for the page using PageGetMaxOffsetNumber
2. Iterates through each offset from FirstOffsetNumber to maxoff
3. Retrieves the ItemId for each offset using PageGetItemId
4. Skips slots that are empty, dead, or redirected
5. Constructs a HeapTupleData structure for each normal item
6. Checks tuple visibility using HeapTupleSatisfiesVisibility with the provided snapshot
7. For visible tuples, calls ScanSourceDatabasePgClassTuple to process the tuple data
8. Adds resulting CreateDBRelInfo objects to the relation list if the tuple represents a relation to copy

The function performs direct page-level access without using the normal heap scanning infrastructure, which is necessary for cross-database access scenarios.

## Parameters / Member Variables
- : Page structure containing the pg_class page data
- : Buffer containing the page (used for visibility checks and block number)
- : Tablespace ID of the source database's default tablespace
- : Database ID of the source database
- : Filesystem path to the source database directory
- : Existing list of relations to copy (may be NIL initially)
- : Snapshot to use for visibility checking

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md): Gets block number from buffer for tuple addressing
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md): Gets highest offset number on the page
  - [PageGetItemId](../P/PageGetItemId.md): Retrieves ItemId for a specific offset
  - ItemIdIsUsed/ItemIdIsDead/ItemIdIsRedirected/ItemIdIsNormal: ItemId state checks
  - [ItemPointerSet](../I/ItemPointerSet.md): Sets tuple's self-pointer (TID)
  - [PageGetItem](../P/PageGetItem.md): Gets actual item data from page
  - ItemIdGetLength: Gets length of item data
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md): Checks if tuple is visible to snapshot
  - [ScanSourceDatabasePgClassTuple](ScanSourceDatabasePgClassTuple.md): Processes individual pg_class tuples
  - lappend: Adds elements to list
- Called from (representative examples):
  - [ScanSourceDatabasePgClass](ScanSourceDatabasePgClass.md): Uses this to process each page of pg_class

## Notes and Other Information
- This function performs direct page-level tuple access, bypassing normal heap scan methods
- Uses low-level ItemId manipulation to iterate through page contents
- Properly handles different ItemId states (used, dead, redirected, normal)
- Constructs HeapTupleData manually since normal heap access methods aren't available
- Relies on snapshot-based visibility checking to ensure consistent view of data
- Returns updated rlocatorlist with newly discovered relations appended
- Part of the cross-database access mechanism used during database creation
- Located at src/backend/commands/dbcommands.c:328-390