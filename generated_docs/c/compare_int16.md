# compare_int16

## Location
src/backend/commands/statscmds.c: 49 - 61

## Overview
A qsort comparator function that compares two int16 (16-bit integer) values for ascending order sorting purposes.

## Definition


## Detailed Description
This is a standard qsort comparison function that takes two void pointers, casts them to int16 pointers, dereferences them to get the actual int16 values, and returns the difference. The function is designed to be used with qsort() for sorting arrays of int16 values in ascending order. The implementation uses a simple subtraction approach which is safe because the comment explicitly notes that "this can't overflow if int is wider than int16", which is true on all modern systems where int is typically 32 bits while int16 is 16 bits.

## Parameters / Member Variables
- : Pointer to the first int16 value to compare (cast from void*)
- : Pointer to the second int16 value to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic pointer dereferencing and arithmetic)
- Called from (representative examples):
  - [publication_translate_columns](../p/publication_translate_columns.md) (src/backend/catalog/pg_publication.c:554)
  - [CreateStatistics](../C/CreateStatistics.md) (src/backend/commands/statscmds.c:407)

## Notes and Other Information
- This is a static function, meaning it's only visible within the pg_publication.c compilation unit
- The function returns a negative value if a < b, zero if a == b, and positive if a > b, following standard qsort comparator conventions
- The subtraction approach (av - bv) is safe from integer overflow on systems where int is wider than int16
- Commonly used for sorting attribute numbers (attnums) in PostgreSQL's publication and statistics subsystems