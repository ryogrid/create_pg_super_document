# mbbisearch

## Location
[src/common/wchar.c:581-627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L581-L627)

## Overview
An auxiliary binary search function that determines whether a Unicode character falls within any interval in a sorted table of character ranges.

## Definition
```c
static int mbbisearch(pg_wchar ucs, const struct mbinterval *table, int max)
```

## Detailed Description
This function implements a binary search algorithm to efficiently search through a sorted table of Unicode character intervals (ranges). It is designed to determine whether a given Unicode character (represented as pg_wchar/UCS-4) falls within any of the character ranges defined in the interval table.

The function performs the following operations:
1. First, it performs a quick bounds check to see if the character falls outside the entire range covered by the table
2. If within bounds, it uses binary search to locate the appropriate interval
3. For each interval tested, it checks if the character falls between the `first` and `last` values (inclusive)
4. Returns 1 if the character is found within any interval, 0 otherwise

This implementation is particularly useful for character classification tasks, such as determining if a character has zero width, is a combining character, or belongs to other special Unicode categories.

## Parameters / Member Variables
- `ucs`: The Unicode character (pg_wchar) to search for in the interval table
- `table`: Pointer to a sorted array of mbinterval structures defining character ranges
- `max`: The index of the last element in the table (0-based indexing)

## Dependencies
- Functions called/Symbols referenced:
  - struct mbinterval (structure definition)
- Called from (representative examples):
  - [ucs_wcwidth](../u/ucs_wcwidth.md)

## Notes and Other Information
- This is a static function with internal linkage, only accessible within wchar.c
- The function assumes the input table is sorted by the `first` field of each interval
- Uses efficient binary search with O(log n) time complexity
- Originally derived from Markus Kuhn's wcwidth implementation, customized for PostgreSQL
- Part of the Unicode character width determination infrastructure
- The intervals in the table represent contiguous ranges of Unicode characters that share common properties
- Critical for proper terminal display width calculations and text formatting in PostgreSQL

## Simplified Source

```c
static int mbbisearch(pg_wchar ucs, const struct mbinterval *table, int max) {
    int min = 0;
    int mid;

    // Quick bounds check
    if (ucs < table[0].first || ucs > table[max].last)
        return 0;

    // Binary search for character in interval table
    while (max >= min) {
        mid = (min + max) / 2;
        if (ucs > table[mid].last)
            min = mid + 1;
        else if (ucs < table[mid].first)
            max = mid - 1;
        else
            return 1;  // Found in interval
    }

    return 0;  // Not found
}
```