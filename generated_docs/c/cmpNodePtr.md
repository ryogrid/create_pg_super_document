# cmpNodePtr

## Location
[src/backend/access/spgist/spgtextproc.c:324-332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgtextproc.c#L324-L332)

## Overview
A static comparison function used as a qsort comparator to sort spgNodePtr structures by their 'c' field in ascending order.

## Definition

```c
static int
cmpNodePtr(const void *a, const void *b)
```
## Detailed Description
This function serves as a comparator for the qsort library function, specifically designed to sort arrays of spgNodePtr structures. It implements the standard qsort comparator interface by taking two void pointers, casting them to spgNodePtr pointers, and comparing their 'c' fields using PostgreSQL's signed 16-bit integer comparison utility. This function is critical for organizing node pointers in SP-GiST text processing operations where sorting by character values is required.

## Parameters / Member Variables
- `*a`: Pointer to the first spgNodePtr structure to compare
- `*b`: Pointer to the second spgNodePtr structure to compare
## Dependencies
- Functions called/Symbols referenced:
  - [spgNodePtr](../s/spgNodePtr.md) (structure type)
  - [pg_cmp_s16](../p/pg_cmp_s16.md) (PostgreSQL's 16-bit signed integer comparison function)
- Called from (representative examples):
  - [spg_text_picksplit](../s/spg_text_picksplit.md) (used in qsort operation for sorting node pointers)

## Notes and Other Information
- This is a static function, meaning it has file scope and is only accessible within spgtextproc.c
- The function follows the standard qsort comparator contract: returns negative, zero, or positive values for less-than, equal-to, or greater-than comparisons respectively
- The 'c' field being compared likely represents character values in the context of text processing within SP-GiST indexes

## Simplified Source

```c
static int cmpNodePtr(const void *a, const void *b)
{
    // Cast void pointers to spgNodePtr structures
    const spgNodePtr *aa = (const spgNodePtr *) a;
    const spgNodePtr *bb = (const spgNodePtr *) b;

    // Compare the 'c' fields (likely character values)
    return pg_cmp_s16(aa->c, bb->c);
}
```