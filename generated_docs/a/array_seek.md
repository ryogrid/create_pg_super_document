# array_seek

## Location
[src/backend/utils/adt/arrayfuncs.c:4854-4901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4854-L4901)

## Overview
A static utility function that advances a pointer through an array by skipping over a specified number of array elements, handling both fixed and variable-length types with proper alignment.

## Definition
```c
static char *array_seek(char *ptr, int offset, bits8 *nullbitmap, int nitems,
                       int typlen, bool typbyval, char typalign)
```

## Detailed Description
This function provides efficient pointer arithmetic for traversing PostgreSQL arrays. It handles the complexity of variable-length data types, NULL values, and memory alignment requirements. The function optimizes for the common case of fixed-length types without NULLs by using simple arithmetic, but falls back to element-by-element traversal when dealing with variable-length types or NULL values.

For fixed-length types without NULLs, it performs a simple calculation: `ptr + nitems * aligned_element_size`. For other cases, it iterates through each element, checking the null bitmap when present, and using `att_addlength_pointer` to determine each element's size before advancing the pointer with proper alignment.

The function maintains the null bitmap position and bitmask as it traverses, efficiently handling the bit-packed NULL information.

## Parameters
- `ptr`: Starting location pointer in the array data
- `offset`: 0-based linear element number of the first element (the one at *ptr)
- `nullbitmap`: Pointer to the start of array's null bitmap, or NULL if no null values exist
- `nitems`: Number of array elements to advance over (must be >= 0)
- `typlen`: Type length specification (-1 for variable-length, positive for fixed-length)
- `typbyval`: Boolean indicating whether the type is stored by value
- `typalign`: Character indicating alignment requirement ('c\, 's\, 'i\, 'd\)

## Dependencies
- Functions called/Symbols referenced:
  - bits8 (data type for null bitmap)
  - att_align_nominal (calculates aligned size and pointer)
  - att_addlength_pointer (advances pointer by element length)
- Called from (representative examples):
  - [array_get_element](array_get_element.md)
  - [array_set_element](array_set_element.md)
  - [array_nelems_size](array_nelems_size.md)
  - [array_slice_size](array_slice_size.md)
  - [array_extract_slice](array_extract_slice.md)
  - [array_insert_slice](array_insert_slice.md)

## Notes and Other Information
- Caller is responsible for ensuring nitems is within valid range
- Optimizes for fixed-length types without NULLs using simple arithmetic
- Uses separate loops for NULL and non-NULL cases to improve performance
- Properly maintains null bitmap position and bitmask during traversal
- Handles bitmask overflow when advancing to the next bitmap byte (0x100 check)
- Returns the new pointer position after advancing over the specified elements
- Part of PostgreSQL's internal array support routines
- The function is static, meaning it's only accessible within the arrayfuncs.c compilation unit
- Critical for efficient array operations that need to locate specific elements or ranges