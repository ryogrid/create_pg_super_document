# growalloc

## Location
[src/timezone/zic.c:452-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L452-L472)

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
  - [WORK_AROUND_QTBUG_53071](../W/WORK_AROUND_QTBUG_53071.md)
  - [memory_exhausted](../m/memory_exhausted.md)
  - [size_product](../s/size_product.md)  
  - [erealloc](../e/erealloc.md)

- Called from (representative examples):
  - [inrule](../i/inrule.md)
  - [inzsub](../i/inzsub.md)
  - [inlink](../i/inlink.md)
  - [addtt](../a/addtt.md)

## Notes and Other Information
- This function is static, meaning it's only accessible within src/timezone/zic.c
- Implements sophisticated overflow detection using PTRDIFF_MAX and SIZE_MAX limits
- The growth factor of 1.5x + 1 provides good performance characteristics for dynamic arrays
- Includes special handling for Qt-related issues (WORK_AROUND_QTBUG_53071)
- Returns the existing pointer unchanged if no reallocation is needed
- Critical for safe dynamic array management in timezone rule processing

## Simplified Source

```c
static void *growalloc(void *ptr, size_t itemsize, ptrdiff_t nitems, ptrdiff_t *nitems_alloc) {
    // Return existing pointer if we have enough space
    if (nitems < *nitems_alloc)
        return ptr;

    // Calculate safe allocation limits
    ptrdiff_t nitems_max = PTRDIFF_MAX - WORK_AROUND_QTBUG_53071;
    ptrdiff_t amax = nitems_max < SIZE_MAX ? nitems_max : SIZE_MAX;

    // Check for overflow before growth calculation
    if ((amax - 1) / 3 * 2 < *nitems_alloc)
        memory_exhausted(_("integer overflow"));

    // Grow by 50% + 1 for efficient amortized expansion
    *nitems_alloc += (*nitems_alloc >> 1) + 1;

    // Reallocate with overflow-safe size calculation
    return erealloc(ptr, size_product(*nitems_alloc, itemsize));
}
```