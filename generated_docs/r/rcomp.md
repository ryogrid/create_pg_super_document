# rcomp

## Location
[src/timezone/zic.c:1151-1157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1151-L1157)

## Overview
A comparison function used for sorting rule structures by their rule name in the PostgreSQL timezone handling system.

## Definition
```c
static int rcomp(const void *cp1, const void *cp2)
```

## Detailed Description
The `rcomp` function is a comparison function specifically designed to be used with sorting algorithms (like qsort) to order an array of `struct rule` elements by their rule name. It follows the standard comparison function convention, returning a value less than, equal to, or greater than zero based on whether the first argument's rule name is lexicographically less than, equal to, or greater than the second argument's rule name.

This function is part of the timezone compilation infrastructure in PostgreSQL, where it helps organize timezone rules alphabetically by name for efficient processing and association with timezone zones.

## Parameters / Member Variables
- `cp1`: Pointer to the first `struct rule` element to compare (cast from `const void *`)
- `cp2`: Pointer to the second `struct rule` element to compare (cast from `const void *`)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function)
  - [rule](rule.md) (struct type accessed for r_name member)
- Called from (representative examples):
  - [associate](../a/associate.md)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the src/timezone/zic.c file
- The function follows the standard qsort comparison function signature
- It specifically compares the `r_name` member of rule structures
- Returns the result of strcmp directly, maintaining the standard comparison semantics
- Used in the timezone rule association process to ensure rules are processed in a consistent alphabetical order