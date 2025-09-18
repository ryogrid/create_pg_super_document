# cmp_string

## Location
src/backend/utils/adt/tsquery_op.c: 298 - 306

## Overview
A comparison function used for sorting arrays of string pointers using the standard C qsort function.

## Definition
```c
static int cmp_string(const void *a, const void *b)
```

## Detailed Description
This function serves as a comparison callback for qsort operations on arrays of string pointers. It dereferences the void pointers to obtain the actual string pointers, then performs a lexicographic comparison using the standard strcmp function. The function follows the standard comparison function contract, returning a negative value if the first string is lexicographically less than the second, zero if they are equal, and a positive value if the first is greater than the second.

## Parameters / Member Variables
- `a`: Pointer to the first string pointer (cast from void* to char**)
- `b`: Pointer to the second string pointer (cast from void* to char**)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C string comparison function)
- Called from (representative examples):
  - [tsq_mcontains](../t/tsq_mcontains.md) (at src/backend/utils/adt/tsquery_op.c:322-326, used multiple times for sorting string arrays)

## Notes and Other Information
- This is a static function, only accessible within the tsquery_op.c module
- Follows the standard qsort comparison function signature with void* parameters
- The double pointer dereferencing is necessary because qsort passes pointers to array elements, and the array contains string pointers
- Used specifically in text search query operations for sorting extracted query values to enable efficient comparison algorithms
- The function assumes both parameters are valid non-NULL pointers to string pointers