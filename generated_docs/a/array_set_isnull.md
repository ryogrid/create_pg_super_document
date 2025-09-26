# array_set_isnull

## Location
[src/backend/utils/adt/arrayfuncs.c:4786-4803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4786-L4803)

## Overview
A static utility function that sets the null status of a specific array element by modifying the array's null bitmap.

## Definition
```c
static void array_set_isnull(bits8 *nullbitmap, int offset, bool isNull)
```

## Detailed Description
This function provides a mechanism to update the null status of an array element at a specific position by modifying the array's null bitmap. The function performs bit-level operations to efficiently set or clear the bit corresponding to the specified array element.

The implementation calculates the appropriate byte position and creates a bitmask for the specific bit within that byte. It then uses bitwise operations to either clear the bit (for NULL values) or set the bit (for non-NULL values), following PostgreSQL's bitmap convention where 0 indicates NULL and 1 indicates non-NULL.

## Parameters
- `nullbitmap`: Pointer to the array's null bitmap (must not be NULL - the bitmap must exist)
- `offset`: 0-based linear element number of the array element to modify
- `isNull`: Boolean value indicating the null status to set (true for NULL, false for non-NULL)

## Dependencies
- Functions called/Symbols referenced:
  - bits8 (data type for bitmap representation)
- Called from (representative examples):
  - [array_set_element](array_set_element.md)

## Notes and Other Information
- The function assumes the nullbitmap parameter is not NULL and will crash if passed a NULL bitmap
- Uses efficient bit manipulation: advances the pointer to the correct byte and creates a bitmask for the target bit
- For NULL values (isNull=true): clears the bit using bitwise AND with negated bitmask
- For non-NULL values (isNull=false): sets the bit using bitwise OR with bitmask
- Part of PostgreSQL's internal array support routines
- The function is static, meaning it's only accessible within the arrayfuncs.c compilation unit
- Complements array_get_isnull for complete null bitmap management