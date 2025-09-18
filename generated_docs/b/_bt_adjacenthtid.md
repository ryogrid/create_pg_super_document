# _bt_adjacenthtid

## Location
src/backend/access/nbtree/nbtsplitloc.c: 749 - 787

## Overview
A utility function that determines if two heap tuple identifiers (TIDs) are "adjacent", meaning they were likely inserted consecutively into the heap relation, probably during the current transaction.

## Definition


## Detailed Description
This function implements a heuristic to detect whether two heap TIDs represent adjacent insertions in the heap. The adjacency test is used during B-tree split location decisions to optimize page splits by keeping related tuples together. The function uses two criteria for adjacency:

1. **Same heap block**: If both TIDs reference the same heap block, they are considered adjacent (optimistic assumption)
2. **Sequential blocks with first offset**: If the high TID is on the next heap block and has FirstOffsetNumber as its offset, they are considered adjacent

This adjacency information helps the B-tree split algorithm make better decisions about where to split pages to maintain locality of related data.

## Parameters / Member Variables
- : ItemPointer to the lower (earlier) heap tuple identifier
- : ItemPointer to the higher (later) heap tuple identifier that should be tested for adjacency with lowhtid

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