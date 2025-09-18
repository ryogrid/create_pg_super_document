# RT_DELETE

## Location
src/include/lib/radixtree.h: 2652 - 2687

## Overview
RT_DELETE is a macro that generates the name for the public function used to delete keys from a radix tree data structure in PostgreSQL.

## Definition
```c
#define RT_DELETE RT_MAKE_NAME(delete)
```

The actual function signature when expanded:
```c
RT_SCOPE bool RT_DELETE(RT_RADIX_TREE *tree, uint64 key)
```

## Detailed Description
RT_DELETE is the main public interface for deleting keys from PostgreSQL's radix tree implementation. This macro-generated function provides a clean API for removing 64-bit integer keys from the tree structure. The function performs validation checks, delegates the actual deletion work to RT_DELETE_RECURSIVE, and maintains tree statistics.

The function implements proper error handling by checking if the key is within the tree's valid range before attempting deletion. It also handles shared memory scenarios with appropriate assertion checks. After successful deletion, it updates the tree's key count statistics to maintain accurate metadata.

## Parameters / Member Variables
- `tree`: Pointer to the RT_RADIX_TREE structure from which to delete the key
- `key`: The 64-bit unsigned integer key to be removed from the tree

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - [RT_DELETE_RECURSIVE](RT_DELETE_RECURSIVE.md) (internal recursive deletion function)
  - RT_PTR_ALLOC_IS_VALID (validates pointer allocation)
  - Assert (debugging assertion macro)

- Called from (representative examples):
  - RT_HANDLE (radixtree handle operations)
  - User code that needs to remove keys from radix trees

## Notes and Other Information
- Returns true if the key was found and successfully deleted, false if the key was not present
- The caller is responsible for taking an exclusive lock before calling this function
- Only available when RT_USE_DELETE is defined during compilation
- Automatically maintains tree statistics by decrementing num_keys counter on successful deletion
- Includes validation for shared memory trees with magic number verification
- Part of PostgreSQL's generic radix tree template system, allowing for type-safe implementations across different data types