# range_search

## Location
[src/common/unicode_category.c:481-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L481-L501)

## Overview
Performs a binary search to determine if a given Unicode code point exists within any of the ranges defined in a Unicode range table.

## Definition

```c
static bool
range_search(const pg_unicode_range *tbl, size_t size, pg_wchar code)
```
## Detailed Description
This function implements an efficient binary search algorithm to check whether a Unicode code point falls within any of the ranges specified in a sorted table of Unicode ranges. Each range is defined by a first and last code point, and the function determines if the input code point lies within any of these inclusive ranges. The binary search provides O(log n) time complexity, making it suitable for checking large Unicode property tables. The function includes validation to ensure the code point is within the valid Unicode range (up to U+10FFFF).

## Parameters / Member Variables

.if !dTS .ds TS
.if !dTE .ds TE
.lf 1 -: Pointer to an array of pg_unicode_range structures containing sorted Unicode ranges
- : Number of ranges in the table
- : The Unicode code point (pg_wchar) to search for within the ranges

## Dependencies
- Functions called/Symbols referenced:
  - [pg_unicode_range](../p/pg_unicode_range.md) (structure type for range table entries)
- Called from (representative examples):
  - PG_U_CHARACTER_TAB (character categorization macro)
  - [pg_u_prop_alphabetic](../p/pg_u_prop_alphabetic.md) (alphabetic property checking)
  - [pg_u_prop_lowercase](../p/pg_u_prop_lowercase.md) (lowercase property checking)
  - [pg_u_prop_uppercase](../p/pg_u_prop_uppercase.md) (uppercase property checking)
  - [pg_u_prop_case_ignorable](../p/pg_u_prop_case_ignorable.md) (case ignorable property checking)
  - [pg_u_prop_white_space](../p/pg_u_prop_white_space.md) (whitespace property checking)
  - [pg_u_prop_hex_digit](../p/pg_u_prop_hex_digit.md) (hex digit property checking)
  - [pg_u_prop_join_control](../p/pg_u_prop_join_control.md) (join control property checking)

## Notes and Other Information
- Uses a static function scope, indicating it's an internal utility for the Unicode category module
- Implements standard binary search with range checking: compares code point against range boundaries
- Includes an assertion to validate that input code points don't exceed the maximum Unicode value (U+10FFFF)
- The pg_unicode_range structure contains 'first' and 'last' fields defining inclusive Unicode code point ranges
- Returns true if the code point falls within any range in the table, false otherwise
- Critical for efficient Unicode property lookup in PostgreSQL's character classification system
- The algorithm assumes the input table is sorted by range boundaries for correct binary search operation

## Simplified Source

```c
static bool range_search(const pg_unicode_range *tbl, size_t size, pg_wchar code) {
    int min = 0;
    int max = size - 1;

    Assert(code <= 0x10ffff);  // Valid Unicode range check

    // Standard binary search algorithm
    while (max >= min) {
        int mid = (min + max) / 2;

        if (code > tbl[mid].last)
            min = mid + 1;           // Search upper half
        else if (code < tbl[mid].first)
            max = mid - 1;           // Search lower half
        else
            return true;             // Found: code is within this range
    }

    return false;  // Not found in any range
}
```