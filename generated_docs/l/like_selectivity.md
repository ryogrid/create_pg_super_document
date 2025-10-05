# like_selectivity

## Location
[src/backend/utils/adt/like_support.c:1323-1359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L1323-L1359)

## Overview
Estimates the selectivity of a LIKE pattern by analyzing wildcard and literal characters and multiplying their individual selectivity factors.

## Definition
```c
static Selectivity like_selectivity(const char *patt, int pattlen, bool case_insensitive)
```

## Detailed Description
This function calculates the selectivity estimate for a LIKE pattern by analyzing each character and applying predefined selectivity factors. The algorithm processes the pattern character by character, multiplying selectivity factors based on the character type:

- **Fixed characters**: Apply FIXED_CHAR_SEL factor (most selective)
- **`_` wildcard**: Apply ANY_CHAR_SEL factor (matches any single character)
- **`%` wildcard**: Apply FULL_WILDCARD_SEL factor (least selective, matches any string)
- **Escaped characters**: Characters following backslash are treated as fixed characters

The function skips leading wildcards since they are typically handled by the prefix analysis phase. It includes logic to handle backslash escaping, where a backslash quotes the next character, making it a literal rather than a wildcard.

The final selectivity is clamped to 1.0 to handle cases where multiple wildcards might mathematically produce a value greater than 1.0, which is logically impossible for selectivity.

## Parameters / Member Variables
- `patt`: Pointer to the LIKE pattern string to analyze
- `pattlen`: Length of the pattern string in characters
- `case_insensitive`: Boolean flag indicating whether the pattern matching should be case-insensitive (affects character selectivity estimation)

## Dependencies
- Functions called/Symbols referenced:
  - FULL_WILDCARD_SEL (selectivity constant for % wildcard)
  - ANY_CHAR_SEL (selectivity constant for _ wildcard)  
  - FIXED_CHAR_SEL (selectivity constant for literal characters)
- Called from:
  - [like_fixed_prefix](like_fixed_prefix.md) (when estimating selectivity for the remaining pattern after prefix extraction)

## Notes and Other Information
- This is a static function within like_support.c, not exposed in the public API
- The function ignores the `case_insensitive` parameter in the current implementation, though it could be used to adjust selectivity factors for case-insensitive matching
- Leading wildcards are skipped since they are typically factored into the initial selectivity estimate by the prefix analysis
- Backslash escaping follows LIKE standard behavior where `\%` and `\_` are treated as literal characters
- The selectivity multiplication approach assumes independence between character positions, which is a reasonable approximation for query planning purposes
- The 1.0 clamp prevents mathematical overflow in cases with many wildcards

## Simplified Source
```c
static Selectivity like_selectivity(const char *pattern, int pattern_length, bool case_insensitive) {
    Selectivity selectivity = 1.0;
    int pos;

    // Skip leading wildcards (already handled by prefix analysis)
    for (pos = 0; pos < pattern_length; pos++) {
        if (pattern[pos] != '%' && pattern[pos] != '_')
            break;
    }

    // Process each character in the pattern
    for (; pos < pattern_length; pos++) {
        if (pattern[pos] == '%') {
            // % wildcard: matches any string
            selectivity *= FULL_WILDCARD_SEL;
        }
        else if (pattern[pos] == '_') {
            // _ wildcard: matches any single character
            selectivity *= ANY_CHAR_SEL;
        }
        else if (pattern[pos] == '\\') {
            // Escaped character: treat next char as literal
            pos++;
            if (pos < pattern_length)
                selectivity *= FIXED_CHAR_SEL;
        }
        else {
            // Regular character: literal match
            selectivity *= FIXED_CHAR_SEL;
        }
    }

    // Ensure selectivity doesn't exceed 1.0
    if (selectivity > 1.0)
        selectivity = 1.0;

    return selectivity;
}
```