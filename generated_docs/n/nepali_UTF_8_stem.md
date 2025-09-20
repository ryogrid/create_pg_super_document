# nepali_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_nepali.c:376-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_nepali.c#L376-L417)

## Overview
The main stemming function for Nepali text encoded in UTF-8, implementing the complete Snowball stemming algorithm for the Nepali language.

## Definition
```c
extern int nepali_UTF_8_stem(struct SN_env * z)
```

## Detailed Description
This is the primary entry point for Nepali word stemming in PostgreSQL's text search functionality. The function implements a multi-stage stemming process that progressively removes different categories of suffixes from Nepali words.

The stemming process follows this sequence:
1. **Initialization**: Sets the left boundary (lb) to current position and moves cursor to end of word (c = l)
2. **Category 1 Processing**: Attempts to remove category 1 suffixes (basic suffixes)
3. **Iterative Processing Loop**: Repeatedly processes categories 2 and 3 until no more suffixes can be removed:
   - **Category 2 Processing**: First checks if category 2 patterns are present, then removes them if found
   - **Category 3 Processing**: Removes category 3 suffixes (largest set with 91 patterns)
4. **Finalization**: Restores cursor to left boundary and returns success

The function uses cursor position management with save/restore operations (m1, m2, m3, m4, m5 variables) to enable backtracking when suffix removal attempts fail.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing:

## Dependencies
- Functions called/Symbols referenced:
  - [r_remove_category_1](../r/r_remove_category_1.md) (removes category 1 suffixes)
  - [r_check_category_2](../r/r_check_category_2.md) (checks for category 2 patterns)
  - [r_remove_category_2](../r/r_remove_category_2.md) (removes category 2 suffixes)
  - [r_remove_category_3](../r/r_remove_category_3.md) (removes category 3 suffixes)
- Called from:
  - External callers in PostgreSQL text search system (not directly referenced in this file)

## Notes and Other Information
- This is an extern function, making it accessible to other modules in the PostgreSQL system
- Implements the complete Snowball stemming algorithm for Nepali language
- Uses iterative processing to handle words with multiple suffix layers
- Part of the automatically generated code from Snowball stemming rules
- Returns 1 on successful completion, propagates negative error codes from called functions
- Critical component of PostgreSQL's full-text search capabilities for Nepali language support