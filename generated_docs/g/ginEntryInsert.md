# ginEntryInsert

## Location
[src/backend/access/gin/gininsert.c:176-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gininsert.c#L176-L252)

## Overview
Inserts one or more heap TIDs associated with a given key value into a GIN index, either creating a new entry or enlarging an existing one.

## Definition

```c
struct a new leaf entry */
		itup = buildFreshLeafTuple(ginstate, attnum, key, category,
								   items, nitem, buildStats, stack->buffer);
```
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

## Simplified Source
```c
void ginEntryInsert(GinState *ginstate,
                   OffsetNumber attnum, Datum key, GinNullCategory category,
                   ItemPointerData *items, uint32 nitem,
                   GinStatsData *buildStats) {
    GinBtreeData btree;
    GinBtreeEntryInsertData insertdata;
    GinBtreeStack *stack;
    IndexTuple itup;
    Page page;

    insertdata.isDelete = false;

    // Initialize B-tree scan for the key
    ginPrepareEntryScan(&btree, attnum, key, category, ginstate);
    btree.isBuild = (buildStats != NULL);

    // Find the appropriate leaf page for this key
    stack = ginFindLeafPage(&btree, false, false);
    page = BufferGetPage(stack->buffer);

    if (btree.findItem(&btree, stack)) {
        // Found existing entry for this key
        itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, stack->off));

        if (GinIsPostingTree(itup)) {
            // Entry already has posting tree - insert directly into tree
            BlockNumber rootPostingTree = GinGetPostingTree(itup);

            // Release locks and insert into posting tree
            LockBuffer(stack->buffer, GIN_UNLOCK);
            freeGinBtreeStack(stack);

            ginInsertItemPointers(ginstate->index, rootPostingTree,
                                items, nitem, buildStats);
            return;
        }

        // Entry has posting list - add items to it
        CheckForSerializableConflictIn(ginstate->index, NULL,
                                     BufferGetBlockNumber(stack->buffer));

        itup = addItemPointersToLeafTuple(ginstate, itup, items, nitem,
                                         buildStats, stack->buffer);
        insertdata.isDelete = true;  // Replacing existing tuple
    } else {
        // No existing entry - create new one
        CheckForSerializableConflictIn(ginstate->index, NULL,
                                     BufferGetBlockNumber(stack->buffer));

        itup = buildFreshLeafTuple(ginstate, attnum, key, category,
                                  items, nitem, buildStats, stack->buffer);

        // Increment entry count for new entries only
        if (buildStats)
            buildStats->nEntries++;
    }

    // Insert the new or modified tuple into the B-tree
    insertdata.entry = itup;
    ginInsertValue(&btree, stack, &insertdata, buildStats);
    pfree(itup);
}
```