# intset_begin_iterate

## Location
[src/backend/lib/integerset.c:624-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L624-L642)

## Overview
Initializes an iterator for sequential traversal of all values in an IntegerSet in ascending order, preparing the set for iteration while preventing modifications during the process.

## Definition

```c
void
intset_begin_iterate(IntegerSet *intset)
```
## Detailed Description
The  function sets up the necessary state for iterating through all values stored in an IntegerSet in ascending order. This function initializes the iterator state machine that allows for efficient sequential access to all compressed and uncompressed values in the set.

Key initialization steps include:
1. **Activation flag**: Sets the  flag to true, which prevents new values from being added during iteration
2. **Position setup**: Positions the iterator at the leftmost leaf node, which contains the smallest values in the set
3. **State reset**: Initializes all iterator counters and pointers to their starting positions
4. **Buffer preparation**: Sets up the iterator's value buffer for efficient batch decompression

The function prepares for a complete traversal that will visit buffered values, compressed values in leaf nodes, and handle the decompression of Simple-8b encoded data transparently during iteration.

## Parameters
- : Pointer to the IntegerSet structure to iterate over

## Dependencies
- Functions called/Symbols referenced:
  - [IntegerSet](../I/IntegerSet.md) (structure access)
- Called from (representative examples):
  - [gistvacuum_delete_empty_pages](../g/gistvacuum_delete_empty_pages.md)
  - [test_pattern](../t/test_pattern.md)
  - [test_single_value](../t/test_single_value.md)
  - [test_single_value_and_filler](../t/test_single_value_and_filler.md)
  - [test_empty](../t/test_empty.md)
  - [test_huge_distances](../t/test_huge_distances.md)

## Notes and Other Information
- Prevents modification of the IntegerSet while iteration is active to maintain consistency
- Allows iterations to be abandoned midway without requiring explicit cleanup
- Positions the iterator at the beginning (smallest values) of the set
- Must be called before using other iteration functions like 
- The iterator state includes both position tracking and value decompression buffers
- Iteration will process values in strictly ascending order across the entire set
- No return value - the function always succeeds in initializing the iterator state

## Simplified Source

```c
void
intset_begin_iterate(IntegerSet *intset)
{
    // Set up iterator state for sequential traversal
    intset->iter_active = true;
    intset->iter_node = intset->leftmost_leaf;
    intset->iter_itemno = 0;
    intset->iter_valueno = 0;
    intset->iter_num_values = 0;
    intset->iter_values = intset->iter_values_buf;
}
```