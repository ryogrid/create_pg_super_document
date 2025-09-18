# rt_num_entries

## Location
src/test/modules/test_radixtree/test_radixtree.c: 115 - 124

## Overview
A utility function that returns the number of keys currently stored in a radix tree data structure.

## Definition
```c
static uint64 rt_num_entries(rt_radix_tree *tree)
```

## Detailed Description
This function provides a simple interface to retrieve the current count of keys in a radix tree. It accesses the control structure of the radix tree to return the num_keys field, which maintains the total number of keys stored in the tree. This is a read-only operation that doesn't modify the tree structure.

## Parameters / Member Variables
- `tree`: Pointer to the rt_radix_tree structure whose key count is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - Accesses tree->ctl->num_keys (radix tree control structure member)
- Called from (representative examples):
  - test_empty (in test suite)
  - test_random (in test suite)

## Notes and Other Information
- This is a static function used internally within the test_radixtree module
- Returns uint64 to support large key counts
- Used primarily in test scenarios to verify the correctness of radix tree operations
- The function assumes the tree parameter is valid and non-NULL