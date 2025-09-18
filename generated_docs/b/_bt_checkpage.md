# _bt_checkpage

## Location
[src/backend/access/nbtree/nbtpage.c:797-844](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L797-L844)

## Overview
_bt_checkpage performs basic sanity checks on a freshly-read B-tree page to detect corruption and ensure the page structure is valid for B-tree operations.

## Definition


## Detailed Description
This function provides essential corruption detection for B-tree pages immediately after they are read from storage. It performs two critical validation checks:

1. **Zero Page Detection**: Identifies pages that are all-zero, which indicates corruption since valid B-tree pages should never be completely empty after initialization.

2. **Special Area Validation**: Verifies that the page's special area (where B-tree opaque data is stored) has the correct size for BTPageOpaqueData structure.

The function assumes that ReadBuffer has already validated basic page header integrity via PageHeaderIsValid, so it focuses on B-tree-specific structural requirements.

When corruption is detected, the function reports detailed error information including the index name and block number, and suggests REINDEXing to repair the corruption.

## Parameters / Member Variables
- : The B-tree index relation that owns the page
- : Buffer containing the page to be validated

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md): Gets page from buffer
  - [PageIsNew](../P/PageIsNew.md): Checks if page is all-zero/uninitialized
  - [PageGetSpecialSize](../P/PageGetSpecialSize.md): Gets size of page's special area
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md): Gets block number for error reporting
  - RelationGetRelationName: Gets relation name for error messages
  - [BTPageOpaqueData](../B/BTPageOpaqueData.md): B-tree page opaque structure type
- Called from (representative examples):
  - [_bt_getbuf](_bt_getbuf.md): After reading any B-tree page
  - [_bt_relandgetbuf](_bt_relandgetbuf.md): When switching to a different page
  - [_bt_search_insert](_bt_search_insert.md): During insertion operations
  - [btvacuumpage](btvacuumpage.md): During VACUUM operations

## Notes and Other Information
- Called immediately after reading B-tree pages to catch corruption early
- Relies on ReadBuffer having already performed basic PageHeaderIsValid checks
- Reports corruption with specific block numbers and relation names for debugging
- Suggests REINDEX as the primary recovery method for detected corruption
- Part of PostgreSQL's defense-in-depth strategy against index corruption
- The special area size check ensures the page can hold B-tree opaque data properly
- The function is located in src/backend/access/nbtree/nbtpage.c:797-844