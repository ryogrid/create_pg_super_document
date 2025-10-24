# cmpLexemeQ

## Location
[src/backend/tsearch/dict_thesaurus.c:372-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L372-L377)

## Overview
A qsort-compatible wrapper function for cmpLexeme that enables sorting arrays of TheLexeme structures using standard library sorting functions.

## Definition

```c
static int
cmpLexemeQ(const void *a, const void *b)
```
## Detailed Description
This function serves as an adapter between the specific cmpLexeme comparison function and the generic qsort/bsearch family of functions that require comparators with void pointer parameters. It performs the necessary type casting from void pointers to TheLexeme pointers and delegates the actual comparison logic to cmpLexeme.

This design pattern is commonly used in C to make type-specific comparison functions compatible with generic sorting and searching algorithms that expect function pointers with a standardized signature.

## Parameters / Member Variables
- `a`: Void pointer to the first TheLexeme structure (cast from const void*)
- `b`: Void pointer to the second TheLexeme structure (cast from const void*)

## Dependencies
- Functions called/Symbols referenced:
  - [cmpLexeme](cmpLexeme.md) (the underlying comparison function)
  - [TheLexeme](../T/TheLexeme.md) (structure type for casting)
- Called from (representative examples):
  - [findTheLexeme](../f/findTheLexeme.md)

## Notes and Other Information
- This function follows the standard qsort/bsearch comparator function signature
- Acts as a type-safe wrapper around cmpLexeme for use with generic sorting/searching functions
- Returns the same comparison results as cmpLexeme: negative, zero, or positive values indicating relative ordering
- Used specifically for binary search operations in thesaurus dictionary lookups via findTheLexeme
- The 'Q' suffix typically indicates compatibility with qsort-style comparison functions

## Simplified Source

```c
static int cmpLexemeQ(const void *a, const void *b) {
    // Cast void pointers to TheLexeme and delegate to typed comparison
    return cmpLexeme((const TheLexeme *) a, (const TheLexeme *) b);
}
```