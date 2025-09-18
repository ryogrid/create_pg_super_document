# RT_END_ITERATE

## Location
src/include/lib/radixtree.h: 2268 - 2278

## Overview
RT_END_ITERATE is a macro that expands to a function name for terminating iteration through a radix tree and freeing the iterator resources.

## Definition
```c
#define RT_END_ITERATE RT_MAKE_NAME(end_iterate)
```

Function signature:
```c
RT_SCOPE void RT_END_ITERATE(RT_ITER * iter);
```

## Detailed Description
RT_END_ITERATE is a preprocessor macro that generates a function name for properly terminating radix tree iteration. This function performs cleanup by freeing the memory allocated for the iterator structure that was created by RT_BEGIN_ITERATE.

The function is straightforward and performs only one operation:
1. **Memory Deallocation**: Calls pfree() to release the memory allocated for the RT_ITER structure

This function is part of the resource management pattern for radix tree iteration, ensuring that allocated memory is properly cleaned up when iteration is complete or no longer needed.

## Parameters / Member Variables
- `iter`: Pointer to the RT_ITER structure to be freed. Must be a valid iterator previously created by RT_BEGIN_ITERATE.

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (for name generation)
  - pfree (for memory deallocation)
- Called from (representative examples):
  - User code after completing tree traversal
  - Cleanup code in error handling paths
  - Database scan completion routines

## Notes and Other Information
- Must be called to prevent memory leaks from iterator allocation
- The caller is responsible for releasing any locks held during iteration
- Should be called even if iteration was terminated early (before reaching the end)
- The iterator pointer should not be used after calling this function
- Part of PostgreSQL's generic radix tree implementation located in src/include/lib/radixtree.h:189
- Complements RT_BEGIN_ITERATE in the iteration lifecycle
- Simple but essential for proper resource management