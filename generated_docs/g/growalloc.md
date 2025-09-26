# growalloc

## Location
src/timezone/zic.c: 452 - 472

## Overview
A dynamic memory reallocation function in the PostgreSQL timezone compiler that implements safe array growth with overflow protection.

## Definition
```c
static void *
growalloc(void *ptr, size_t itemsize, ptrdiff_t nitems, ptrdiff_t *nitems_alloc)
```

## Detailed Description
The `growalloc` function provides safe dynamic array expansion with built-in overflow protection. It implements a growth strategy that increases allocation by 50% plus one item when more space is needed. The function includes comprehensive bounds checking to prevent integer overflow conditions that could lead to security vulnerabilities or memory corruption.

The growth algorithm follows the pattern: new_size = old_size + (old_size >> 1) + 1, which provides efficient amortized growth while preventing excessive memory usage. The function also incorporates Qt-specific workarounds and comprehensive overflow detection.

## Parameters / Member Variables
- `ptr`: Pointer to the existing memory block to be reallocated
- `itemsize`: Size in bytes of each individual item in the array  
- `nitems`: Number of items currently needed
- `nitems_alloc`: Pointer to the current number of allocated items (updated by the function)

## Dependencies
- Functions called/Symbols referenced:
  - WORK_AROUND_QTBUG_53071
  - memory_exhausted
  - size_product  
  - erealloc

- Called from (representative examples):
  - inrule
  - inzsub
  - inlink
  - addtt

## Notes and Other Information
- This function is static, meaning it's only accessible within src/timezone/zic.c
- Implements sophisticated overflow detection using PTRDIFF_MAX and SIZE_MAX limits
- The growth factor of 1.5x + 1 provides good performance characteristics for dynamic arrays
- Includes special handling for Qt-related issues (WORK_AROUND_QTBUG_53071)
- Returns the existing pointer unchanged if no reallocation is needed
- Critical for safe dynamic array management in timezone rule processing