# array_get_isnull

## Location
[src/backend/utils/adt/arrayfuncs.c:4769-4785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4769-L4785)

## Overview
A static utility function that checks whether a specific array element is NULL by examining the array's null bitmap.

## Definition
```c
static bool array_get_isnull(const bits8 *nullbitmap, int offset)
```

## Detailed Description
This function provides a mechanism to determine if an array element at a specific position is NULL by examining the array's null bitmap. The null bitmap is a compact bit-packed representation where each bit corresponds to an array element, with 0 indicating NULL and 1 indicating non-NULL values.

The function implements bit-level operations to efficiently check the null status: it calculates the appropriate byte position (`offset / 8`) and bit position within that byte (`offset % 8`) to test the specific bit corresponding to the element in question.

## Parameters
- `nullbitmap`: Pointer to the array's null bitmap (NULL if the array has no null bitmap, meaning all elements are non-NULL)
- `offset`: 0-based linear element number of the array element to check

## Dependencies
- Functions called/Symbols referenced:
  - bits8 (data type for bitmap representation)
- Called from (representative examples):
  - array_get_element
  - array_set_element
  - array_iterate
  - array_slice_size

## Notes and Other Information
- Returns false (not null) if nullbitmap is NULL, assuming all elements are non-NULL when no bitmap exists
- Uses bit manipulation to efficiently check null status in the compact bitmap representation
- Part of PostgreSQL's internal array support routines
- The function is static, meaning it's only accessible within the arrayfuncs.c compilation unit
- The bitmap format stores NULL as 0-bit and non-NULL as 1-bit, which is the standard PostgreSQL convention