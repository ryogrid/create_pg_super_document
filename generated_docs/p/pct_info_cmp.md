# pct_info_cmp

## Location
[src/backend/utils/adt/orderedsetaggs.c:646-661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L646-L661)

## Overview
A static comparison function used for sorting pct_info structures by their first_row and second_row fields in ascending order.

## Definition


## Detailed Description
This function serves as a comparator for sorting pct_info structures in ordered set aggregate operations. It implements a two-level sorting criterion: first by first_row field, then by second_row field if the first_row values are equal. The function is designed to be used with standard library sorting functions like qsort().

The comparison follows standard C library comparator conventions, returning:
- A negative value if the first argument is less than the second
- Zero if the arguments are equal
- A positive value if the first argument is greater than the second

## Parameters / Member Variables
- : Pointer to the first pct_info structure to compare (cast from void*)
- : Pointer to the second pct_info structure to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - [pct_info](pct_info.md) (struct type)
- Called from (representative examples):
  - [setup_pct_info](../s/setup_pct_info.md) (via qsort or similar sorting mechanism)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only visible within the orderedsetaggs.c compilation unit
- The function assumes that both input pointers are valid and point to pct_info structures
- Used specifically in the context of percentile calculations for ordered set aggregates like percentile_disc and percentile_cont
- The sorting order (ascending by first_row, then by second_row) is crucial for the correct operation of percentile aggregate functions