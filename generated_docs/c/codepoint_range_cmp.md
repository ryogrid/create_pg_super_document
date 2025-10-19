# codepoint_range_cmp

## Location
[src/common/saslprep.c:973-986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/saslprep.c#L973-L986)

## Overview
A static comparison function used for searching Unicode codepoint ranges via binary search, specifically designed to determine if a given codepoint falls within a specified range.

## Definition

```c
static int
codepoint_range_cmp(const void *a, const void *b)
```
## Detailed Description
This function serves as a comparison callback for binary search operations (typically used with bsearch()). It compares a Unicode codepoint against a range of codepoints to determine the relative position. The function is specifically designed to work with Unicode codepoint tables that store ranges as pairs of values [lower_bound, upper_bound].

The function returns:
- Negative value (-1) if the codepoint is below the range
- Positive value (1) if the codepoint is above the range  
- Zero (0) if the codepoint falls within the range (inclusive)

This three-way comparison enables efficient binary search through sorted arrays of Unicode codepoint ranges, which is essential for Unicode normalization and character classification operations in SASL string preparation.

## Parameters / Member Variables
- `*a`: Pointer to the search key (pg_wchar codepoint being searched for)
- `*b`: Pointer to a codepoint range array where range[0] is the lower bound and range[1] is the upper bound
## Dependencies
- Functions called/Symbols referenced:
  - (none - uses only basic comparison operations)
- Called from (representative examples):
  - [is_code_in_table](../i/is_code_in_table.md) (via bsearch callback)

## Notes and Other Information
- This is a static function local to src/common/saslprep.c
- Designed specifically for use as a bsearch() comparison callback
- Works with pg_wchar type which represents Unicode codepoints
- Critical component of SASL string preparation Unicode processing
- The range comparison is inclusive on both bounds

## Simplified Source

```c
static int codepoint_range_cmp(const void *a, const void *b) {
    const pg_wchar *key = (const pg_wchar *) a;
    const pg_wchar *range = (const pg_wchar *) b;

    // Check if codepoint is below range
    if (*key < range[0])
        return -1;

    // Check if codepoint is above range
    if (*key > range[1])
        return 1;

    // Codepoint is within range
    return 0;
}
```