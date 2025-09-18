# compare_lexeme_textfreq

## Location
[src/backend/tsearch/ts_selfuncs.c:434-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_selfuncs.c#L434-L452)

## Overview
A bsearch() comparator function that compares a lexeme (non-NULL terminated string with length) against a TextFreq structure for binary searching through text search statistics.

## Definition
```c
static int compare_lexeme_textfreq(const void *e1, const void *e2)
```

## Detailed Description
This function serves as a comparison function for the C library's `bsearch()` routine, specifically designed to find lexemes within PostgreSQL's text search statistics. It compares a LexemeKey (containing a lexeme string and its length) with a TextFreq structure (containing a text element and frequency information).

The comparison follows a two-step process:
1. **Length comparison**: First compares the lengths of the lexeme and the text element, returning immediately if they differ
2. **Byte-for-byte comparison**: If lengths are equal, performs a `strncmp()` to compare the actual string content

This comparison strategy matches exactly how the ANALYZE code sorted data before storing it in statistic tuples, ensuring consistency with the data organization used by PostgreSQL's text search statistics collection.

## Parameters / Member Variables
- `e1`: Pointer to a LexemeKey structure containing the search key (lexeme and length)
- `e2`: Pointer to a TextFreq structure containing a text element and its frequency

## Dependencies
- Functions called/Symbols referenced:
  - `VARSIZE_ANY_EXHDR`: Macro to get the data size of a variable-length type
  - `VARDATA_ANY`: Macro to get the data portion of a variable-length type
  - `strncmp`: Standard C string comparison function
- Called from (representative examples):
  - [tsquery_opr_selec](../t/tsquery_opr_selec.md): Used in `bsearch()` calls to find lexemes in MCELEM statistics for text search selectivity estimation

## Notes and Other Information
- This is a static function used internally within ts_selfuncs.c for text search selectivity calculations
- The function is specifically designed to work with PostgreSQL's Most Common Elements (MCELEM) statistics for tsvector columns
- The comparison logic ensures that lexemes are found correctly in the pre-sorted statistics arrays created during ANALYZE
- Used in conjunction with binary search to efficiently locate specific lexemes in text search statistics for query planning and selectivity estimation
- The function handles PostgreSQL's variable-length text types (varlena) through the VARSIZE_ANY_EXHDR and VARDATA_ANY macros