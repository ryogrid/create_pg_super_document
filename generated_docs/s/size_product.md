# size_product

## Location
[src/timezone/zic.c:418-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L418-L425)

## Overview
A static utility function that safely computes the product of two size_t values while checking for integer overflow to prevent memory allocation errors.

## Definition
```c
static size_t size_product(size_t nitems, size_t itemsize)
```

## Detailed Description
The `size_product` function performs safe multiplication of two size_t values, specifically designed to prevent integer overflow when calculating memory allocation sizes. It implements overflow detection by checking if the division of SIZE_MAX by itemsize is less than nitems before performing the multiplication. If overflow would occur, it calls `memory_exhausted` to terminate the program with an appropriate error message. This function is crucial for preventing buffer overflows and memory corruption that could result from integer overflow during size calculations for dynamic memory allocation.

## Parameters / Member Variables
- `nitems`: The number of items for which to calculate total size
- `itemsize`: The size in bytes of each individual item

## Dependencies
- Functions called/Symbols referenced:
  - memory_exhausted (for handling overflow conditions)
  - SIZE_MAX (standard constant representing maximum value for size_t)
  - _ (gettext macro for internationalization)
- Called from (representative examples):
  - growalloc
  - writezone
  - getfields

## Notes and Other Information
- This is a static function, accessible only within the src/timezone/zic.c file
- Implements a common defensive programming pattern for safe integer arithmetic
- The overflow check uses division instead of multiplication to avoid actually causing overflow during the check
- Essential for preventing security vulnerabilities related to integer overflow in memory allocation
- Returns the product only if it can be safely computed without overflow