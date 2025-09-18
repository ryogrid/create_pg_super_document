# RT_SET

## Location
src/include/lib/radixtree.h: 1705 - 1821

## Overview
RT_SET is the primary function for inserting or updating key-value pairs in a radix tree, handling tree extension and value storage management.

## Definition
```c
RT_SCOPE bool RT_SET(RT_RADIX_TREE * tree, uint64 key, RT_VALUE_TYPE * value_p)
```

## Detailed Description
This function sets a key to the value pointed to by value_p. If an entry already exists, it updates the value and returns true; otherwise, it creates a new entry and returns false. The function handles several complex scenarios: extending the tree structure upward or downward when the key exceeds current capacity, managing different value storage strategies (embeddable vs. leaf nodes), handling value size changes for existing entries, and maintaining tree statistics. It supports both direct value embedding in child pointer slots for small values and separate leaf allocation for larger values.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure being modified
- `key`: The 64-bit key to insert or update
- `value_p`: Pointer to the value data to be stored

## Dependencies
- Functions called/Symbols referenced:
  - RT_GET_VALUE_SIZE
  - RT_PTR_ALLOC_IS_VALID
  - [RT_KEY_GET_SHIFT](RT_KEY_GET_SHIFT.md)
  - [RT_PTR_SET_LOCAL](RT_PTR_SET_LOCAL.md)
  - RT_GET_KEY_CHUNK
  - [RT_EXTEND_DOWN](RT_EXTEND_DOWN.md)
  - [RT_SHIFT_GET_MAX_VAL](RT_SHIFT_GET_MAX_VAL.md)
  - [RT_EXTEND_UP](RT_EXTEND_UP.md)
  - [RT_GET_SLOT_RECURSIVE](RT_GET_SLOT_RECURSIVE.md)
  - [RT_VALUE_IS_EMBEDDABLE](RT_VALUE_IS_EMBEDDABLE.md)
  - [RT_CHILDPTR_IS_VALUE](RT_CHILDPTR_IS_VALUE.md)
  - [RT_FREE_LEAF](RT_FREE_LEAF.md)
  - [RT_ALLOC_LEAF](RT_ALLOC_LEAF.md)
- Called from (representative examples):
  - RT_HANDLE (as referenced)

## Notes and Other Information
- Requires exclusive lock to be held by the caller for thread safety
- Supports both embedded values (stored directly in pointer slots) and leaf-allocated values
- Automatically extends tree structure when keys exceed current maximum value
- Handles value size changes by freeing and reallocating leaves when necessary
- Updates tree statistics (num_keys) for new insertions
- Returns boolean indicating whether the entry previously existed (true) or was newly created (false)
- Critical entry point for all radix tree modifications