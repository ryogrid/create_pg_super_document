# commonPrefix

## Location
[src/test/modules/spgist_name_ops/spgist_name_ops.c:77-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/spgist_name_ops/spgist_name_ops.c#L77-L96)

## Overview
A utility function that finds the length of the common prefix shared between two character strings.

## Definition

```c
static int
commonPrefix(const char *a, const char *b, int lena, int lenb)
```
## Detailed Description
This function compares two character strings byte-by-byte from the beginning and returns the length of their common prefix. It stops comparison when it encounters the first differing character or reaches the end of either string. This is a fundamental operation used in SP-GiST text processing for determining how much of a prefix can be shared between text values, which is crucial for building efficient search tree structures.

The function performs a simple linear scan, making it efficient for typical use cases where common prefixes are relatively short. It respects the length limits of both input strings to avoid buffer overruns.

## Parameters / Member Variables
- `a`: Pointer to the first character string
- `b`: Pointer to the second character string  
- `lena`: Length of the first string in bytes
- `lenb`: Length of the second string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - No external function calls (uses only basic pointer arithmetic and comparison)
- Called from (representative examples):
  - [spg_text_choose](../s/spg_text_choose.md)
  - [spg_text_picksplit](../s/spg_text_picksplit.md)
  - [spgist_name_choose](../s/spgist_name_choose.md)

## Notes and Other Information
- Located in src/backend/access/spgist/spgtextproc.c:138-157
- Static function, only accessible within the same compilation unit
- Time complexity: O(min(lena, lenb)) in worst case, O(k) where k is common prefix length in typical case
- Does not assume null-terminated strings; relies on explicit length parameters
- Essential for SP-GiST operations that need to determine optimal split points and prefix handling
- Returns 0 if strings have no common prefix or if either string is empty

## Simplified Source

```c
static int commonPrefix(const char *a, const char *b, int lena, int lenb)
{
    int i = 0;

    // Compare characters until difference found or end of either string
    while (i < lena && i < lenb && *a == *b) {
        a++;
        b++;
        i++;
    }

    return i;  // Length of common prefix
}
```