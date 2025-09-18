# ReadBuffer

## Location
src/backend/storage/buffer/bufmgr.c: 745 - 791

## Overview
Simplified interface for reading relation blocks using default parameters and main fork access.

## Definition
Buffer ReadBuffer(Relation reln, BlockNumber blockNum)

## Detailed Description
ReadBuffer serves as a convenience wrapper around ReadBufferExtended, providing a simplified interface for the most common buffer reading scenarios. It automatically uses the main fork (MAIN_FORKNUM) of the relation, normal read mode (RBM_NORMAL), and the default buffer access strategy. This function eliminates the need for callers to specify these common parameters explicitly, reducing code complexity while maintaining full functionality.

The function directly delegates to ReadBufferExtended with predetermined parameters, ensuring consistent behavior with the underlying buffer management system. It represents the standard way to read data blocks from relations in most PostgreSQL operations.

## Parameters / Member Variables
- reln: Relation structure representing the target relation to read from
- blockNum: Block number within the main fork to read

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBufferExtended](ReadBufferExtended.md): Core buffer reading implementation
  - MAIN_FORKNUM: Constant for main fork identifier
  - RBM_NORMAL: Constant for normal read mode
- Called from (representative examples):
  - [brinGetStats](../b/brinGetStats.md): BRIN index statistics collection
  - [brin_getinsertbuffer](../b/brin_getinsertbuffer.md): BRIN index buffer allocation
  - [ginFindLeafPage](../g/ginFindLeafPage.md): GIN index leaf page access
  - [gistdoinsert](../g/gistdoinsert.md): GiST index insertion operations
  - [heap_fetch](../h/heap_fetch.md): Heap tuple retrieval
  - [RelationGetBufferForTuple](RelationGetBufferForTuple.md): Heap tuple storage
  - [_bt_search_insert](../b/_bt_search_insert.md): B-tree insertion operations
  - Many other index and table access operations

## Notes and Other Information
- Most commonly used buffer reading function in PostgreSQL codebase
- Automatically handles page validation and error reporting through RBM_NORMAL mode
- Returns pinned buffer that must be released by caller
- Suitable for standard data access patterns where special modes are not required
- Part of the layered buffer management API design for ease of use