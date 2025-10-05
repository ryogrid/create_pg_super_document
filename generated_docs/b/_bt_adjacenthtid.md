# _bt_adjacenthtid

## Location
[src/backend/access/nbtree/nbtsplitloc.c:749-787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsplitloc.c#L749-L787)

## Overview
A utility function that determines if two heap tuple identifiers (TIDs) are "adjacent", meaning they were likely inserted consecutively into the heap relation, probably during the current transaction.

## Definition

```c
static bool
_bt_adjacenthtid(ItemPointer lowhtid, ItemPointer highhtid)
```
## Detailed Description
This function implements a heuristic to detect whether two heap TIDs represent adjacent insertions in the heap. The adjacency test is used during B-tree split location decisions to optimize page splits by keeping related tuples together. The function uses two criteria for adjacency:

1. **Same heap block**: If both TIDs reference the same heap block, they are considered adjacent (optimistic assumption)
2. **Sequential blocks with first offset**: If the high TID is on the next heap block and has FirstOffsetNumber as its offset, they are considered adjacent

This adjacency information helps the B-tree split algorithm make better decisions about where to split pages to maintain locality of related data.

## Parameters / Member Variables
- `lowhtid`: ItemPointer to the lower (earlier) heap tuple identifier
- `highhtid`: ItemPointer to the higher (later) heap tuple identifier that should be tested for adjacency with lowhtid
## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)  
  - FirstOffsetNumber
- Called from (representative examples):
  - FindSplitData
  - [_bt_afternewitemoff](_bt_afternewitemoff.md)

## Notes and Other Information
- This is a static function only used within nbtsplitloc.c for B-tree split optimization
- The adjacency test is heuristic-based and makes optimistic assumptions to improve performance
- Adjacent TIDs typically indicate related data that should be kept together during page splits
- The function helps maintain good clustering properties in B-tree indexes by preserving heap insertion order locality

## Simplified Source
```c
static bool
_bt_adjacenthtid(ItemPointer lowhtid, ItemPointer highhtid)
{
    BlockNumber lowblk = ItemPointerGetBlockNumber(lowhtid);
    BlockNumber highblk = ItemPointerGetBlockNumber(highhtid);

    // Case 1: Same heap block - assume adjacent
    if (lowblk == highblk)
        return true;

    // Case 2: Sequential blocks, high TID is first item on next block
    if (lowblk + 1 == highblk &&
        ItemPointerGetOffsetNumber(highhtid) == FirstOffsetNumber)
        return true;

    // Not adjacent
    return false;
}
```