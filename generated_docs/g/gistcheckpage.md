# gistcheckpage

## Location
[src/backend/access/gist/gistutil.c:784-822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L784-L822)

## Overview
gistcheckpage is a validation function that verifies the integrity and consistency of a freshly-read GiST index page to detect corruption.

## Definition
```c
void gistcheckpage(Relation rel, Buffer buf)
```

## Detailed Description
This function performs sanity checks on GiST index pages after they are read from disk to ensure they are properly formatted and not corrupted. It validates two key aspects: first, it checks if the page is unexpectedly all-zero (which would indicate an uninitialized page in an existing index), and second, it verifies that the special area size matches the expected size for GiST pages. If either check fails, it reports an index corruption error and suggests reindexing.

## Parameters / Member Variables
- `rel`: Relation pointer representing the GiST index relation being checked
- `buf`: Buffer containing the page to be validated

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md) (to extract the page from the buffer)
  - [PageIsNew](../P/PageIsNew.md) (to check if page is all-zero/uninitialized)
  - RelationGetRelationName (to get relation name for error messages)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (to get block number for error messages)
  - [PageGetSpecialSize](../P/PageGetSpecialSize.md) (to verify special area size)
  - MAXALIGN (macro for size alignment)
  - [GISTPageOpaqueData](../G/GISTPageOpaqueData.md) (struct type for size comparison)
- Called from (representative examples):
  - [gistdoinsert](gistdoinsert.md) (during tuple insertion)
  - [gistFindPath](gistFindPath.md) (during tree traversal)
  - [gistNewBuffer](gistNewBuffer.md) (after reading a buffer)
  - [gistkillitems](gistkillitems.md) (during tuple deletion)
  - [gistvacuum_delete_empty_pages](gistvacuum_delete_empty_pages.md) (during vacuum operations)

## Notes and Other Information
- Essential for detecting index corruption early in operations
- Helps maintain data integrity by catching corrupted pages before they cause further damage
- The function assumes that PageHeaderIsValid has already been called by ReadBuffer
- Reports specific error codes (ERRCODE_INDEX_CORRUPTED) to help with diagnosis
- Used extensively throughout GiST operations as a defensive programming measure
- The special area size check ensures the page has the correct GiST-specific metadata structure