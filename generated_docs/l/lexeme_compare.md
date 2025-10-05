# lexeme_compare

## Location
[src/backend/tsearch/ts_typanalyze.c:500-517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_typanalyze.c#L500-L517)

## Overview
A static comparison function for lexemes used in text search analysis that compares LexemeHashKey structures based on length and lexeme content.

## Definition
static int lexeme_compare(const void *key1, const void *key2)

## Detailed Description
This function implements a lexeme comparison algorithm for PostgreSQL's text search functionality. It first compares lexemes by their length, and if lengths are equal, performs a byte-by-byte comparison of the lexeme strings. The function is designed to work with the qsort family of functions, returning standard comparison values (-1, 0, 1) to establish ordering between lexeme keys.

## Parameters / Member Variables
- key1: Pointer to the first LexemeHashKey structure to compare
- key2: Pointer to the second LexemeHashKey structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - LexemeHashKey (struct type)
  - strncmp (standard C library function)
- Called from (representative examples):
  - [trackitem_compare_lexemes](../t/trackitem_compare_lexemes.md)
  - [lexeme_match](lexeme_match.md)

## Notes and Other Information
- Returns 1 if key1 > key2, -1 if key1 < key2, and 0 if equal
- Comparison is first by length, then lexicographically by content
- Used in sorting and searching operations for text search statistics
- The lexeme strings are not NULL-terminated, hence the use of strncmp with explicit length
- Located in src/backend/tsearch/ts_typanalyze.c:500-517

## Simplified Source

```c
static int
lexeme_compare(const void *key1, const void *key2)
{
    const LexemeHashKey *d1 = (const LexemeHashKey *) key1;
    const LexemeHashKey *d2 = (const LexemeHashKey *) key2;

    // Compare by length first
    if (d1->length > d2->length)
        return 1;
    else if (d1->length < d2->length)
        return -1;

    // Lengths equal, compare content byte-by-byte
    return strncmp(d1->lexeme, d2->lexeme, d1->length);
}
```