# intset_flush_buffered_values

## Location
[src/backend/lib/integerset.c:396-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L396-L480)

## Overview
A static internal function that transfers buffered integer values from memory into the compressed B-tree structure of an IntegerSet, using Simple-8b encoding for optimal storage.

## Definition


## Detailed Description
The  function is responsible for converting buffered integer values into compressed leaf items within the IntegerSet's B-tree structure. This function implements the core compression logic by taking raw 64-bit integers and packing them using Simple-8b encoding.

The function handles several critical scenarios:
1. **Empty tree initialization**: When the tree is completely empty, it creates the first leaf node which also serves as the root
2. **Batch compression**: It processes buffered values in batches, ensuring there are enough values (MAX_VALUES_PER_LEAF_ITEM) to efficiently encode
3. **Node management**: When leaf nodes become full (MAX_LEAF_ITEMS), it creates new leaf nodes and updates the B-tree structure
4. **Buffer management**: It moves any remaining unprocessed values to the beginning of the buffer

The function uses Simple-8b encoding to compress sequences of integers relative to a base value, maximizing storage efficiency while maintaining fast access patterns.

## Parameters
- : Pointer to the IntegerSet structure whose buffered values will be flushed to the B-tree

## Dependencies
- Functions called/Symbols referenced:
  - [intset_new_leaf_node](intset_new_leaf_node.md)
  - [simple8b_encode](../s/simple8b_encode.md)
  - [intset_update_upper](intset_update_upper.md)
  - [intset_leaf_node](intset_leaf_node.md)
  - [intset_node](intset_node.md)
  - [leaf_item](../l/leaf_item.md)
  - MAX_VALUES_PER_LEAF_ITEM
  - MAX_LEAF_ITEMS
- Called from (representative examples):
  - [intset_add_member](intset_add_member.md)
  - [IntegerSet](../I/IntegerSet.md) (during finalization)

## Notes and Other Information
- This is a static function, only accessible within the integerset.c file
- Implements lazy compression strategy - values are buffered until there are enough to compress efficiently
- Automatically handles B-tree structure updates when new leaf nodes are needed
- Uses memmove to efficiently relocate remaining buffered values after partial processing
- Critical for maintaining the performance characteristics of the IntegerSet data structure
- The function ensures that the rightmost_nodes array is kept up-to-date for efficient future insertions