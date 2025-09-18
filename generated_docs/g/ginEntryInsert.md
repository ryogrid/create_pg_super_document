# ginEntryInsert

## Location
[src/backend/access/gin/gininsert.c:176-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gininsert.c#L176-L252)

## Overview
Inserts one or more heap TIDs associated with a given key value into a GIN index, either creating a new entry or enlarging an existing one.

## Definition


## Detailed Description
This function is the main entry point for inserting item pointers into a GIN index for a specific key. It handles three different scenarios based on what it finds in the index:

1. **Existing posting tree**: If the key already exists and has a posting tree, it directly inserts the new items into that tree using ginInsertItemPointers.

2. **Existing posting list**: If the key exists with a posting list (not a tree), it calls addItemPointersToLeafTuple to merge the new items with the existing list, potentially converting to a posting tree if needed.

3. **New key**: If the key doesn't exist, it creates a completely new leaf tuple using buildFreshLeafTuple.

The function manages the B-tree traversal to locate the appropriate leaf page, handles serializable conflict detection, and properly manages buffer locks and memory. It also tracks statistics during index builds when buildStats is provided.

## Parameters / Member Variables
- : GIN access method state information containing index configuration
- : Attribute number being indexed (for multi-column indexes)
- : The key value being inserted
- : Category for null value handling (GIN_CAT_NORM_KEY, etc.)
- : Array of item pointers (TIDs) to associate with the key
- : Number of item pointers in the items array
- : Statistics structure for index build operations (NULL during regular inserts)

## Dependencies
- Functions called/Symbols referenced:
  - [ginPrepareEntryScan](ginPrepareEntryScan.md): Initialize B-tree scan for entry operations
  - [ginFindLeafPage](ginFindLeafPage.md): Navigate B-tree to find appropriate leaf page
  - GinIsPostingTree: Check if tuple contains posting tree reference
  - GinGetPostingTree: Extract posting tree block number from tuple
  - [ginInsertItemPointers](ginInsertItemPointers.md): Insert items directly into posting tree
  - [addItemPointersToLeafTuple](../a/addItemPointersToLeafTuple.md): Add items to existing posting list tuple
  - [buildFreshLeafTuple](../b/buildFreshLeafTuple.md): Create new leaf tuple from scratch
  - [ginInsertValue](ginInsertValue.md): Insert tuple into B-tree page
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md): Handle serializable transaction conflicts
  - [freeGinBtreeStack](../f/freeGinBtreeStack.md): Clean up B-tree traversal stack

- Called from (representative examples):
  - [ginBuildCallback](ginBuildCallback.md): During index creation from existing data
  - [ginHeapTupleInsert](ginHeapTupleInsert.md): For regular tuple insertions
  - [ginInsertCleanup](ginInsertCleanup.md): During fast insert cleanup operations
  - [ginbuild](ginbuild.md): Direct calls during index build process

## Notes and Other Information
- Central coordination function for all GIN index insertions
- Handles serializable conflict detection to maintain transaction isolation
- Properly manages buffer locks, releasing them when transitioning to posting tree operations
- Updates entry count statistics only when creating new entries (not when enlarging existing ones)
- Memory management: Allocates new tuple and frees it after insertion
- The isDelete flag in insertdata indicates whether this is replacing an existing tuple
- Critical performance path for GIN index maintenance and construction