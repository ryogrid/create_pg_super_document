# conv_compare

## Location
[src/common/unicode_norm.c:52-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L52-L71)

## Overview
A comparison function used by bsearch() to locate entries in the Unicode decomposition lookup table by comparing codepoints.

## Definition

```c
static int
conv_compare(const void *p1, const void *p2)
```
## Detailed Description
 is a comparison function specifically designed for use with the standard library's  function. It compares a target Unicode codepoint (passed as ) with the codepoint field of a  structure (passed as ). The function implements the standard comparison semantics required by , returning negative, zero, or positive values to indicate the relative ordering of the compared elements.

This function is essential for efficiently searching the decomposition lookup table, which contains Unicode normalization data organized by codepoint values.

## Parameters / Member Variables
- : Pointer to the target Unicode codepoint (uint32) being searched for
- : Pointer to a  structure from the lookup table

## Dependencies
- Functions called/Symbols referenced:
  - [pg_unicode_decomposition](../p/pg_unicode_decomposition.md) (struct type)
- Called from (representative examples):
  - [get_code_entry](../g/get_code_entry.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the  file
- The function follows the standard comparison function interface required by 
- Returns 1 if p1 > p2, 0 if p1 == p2, and -1 if p1 < p2
- The comparison is based on Unicode codepoint values, enabling binary search through the decomposition table

## Simplified Source

```c
static int
conv_compare(const void *p1, const void *p2)
{
    uint32 v1 = *(const uint32 *) p1;
    uint32 v2 = ((const pg_unicode_decomposition *) p2)->codepoint;

    return (v1 > v2) ? 1 : ((v1 == v2) ? 0 : -1);
}
```