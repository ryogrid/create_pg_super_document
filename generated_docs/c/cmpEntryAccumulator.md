# cmpEntryAccumulator

## Location
src/backend/access/gin/ginbulk.c: 72 - 84

## Overview
A comparator function for rbtree.c that compares two GinEntryAccumulator nodes during GIN index bulk loading operations.

## Definition
```c
static int cmpEntryAccumulator(const RBTNode *a, const RBTNode *b, void *arg)
```

## Detailed Description
This function serves as a comparison callback for the red-black tree implementation used during GIN index bulk loading. It compares two GinEntryAccumulator entries by casting the RBTNode parameters to GinEntryAccumulator structures and then using ginCompareAttEntries to perform the actual comparison based on attribute number, key value, and category. The function enables the red-black tree to maintain sorted order of entry accumulators during the bulk loading process.

## Parameters / Member Variables
- `a`: Pointer to the first RBTNode to compare (cast to GinEntryAccumulator)
- `b`: Pointer to the second RBTNode to compare (cast to GinEntryAccumulator) 
- `arg`: Void pointer to BuildAccumulator context containing GIN state information

## Dependencies
- Functions called/Symbols referenced:
  - [ginCompareAttEntries](../g/ginCompareAttEntries.md)
- Called from (representative examples):
  - [ginInitBA](../g/ginInitBA.md)

## Notes and Other Information
- This is a static function used internally within the GIN bulk loading module
- The function relies on ginCompareAttEntries to perform the actual comparison logic
- Part of the GIN access method's bulk loading optimization strategy using red-black trees