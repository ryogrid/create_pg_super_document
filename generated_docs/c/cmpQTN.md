# cmpQTN

## Location
[src/backend/utils/adt/tsquery_util.c:153-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L153-L162)

## Overview
A qsort-compatible wrapper function that enables sorting arrays of QTNode pointers using the QTNodeCompare comparison logic.

## Definition

```c
static int
cmpQTN(const void *a, const void *b)
```
## Detailed Description
cmpQTN is a static wrapper function designed to adapt QTNodeCompare for use with the standard C qsort function. It follows the qsort comparator interface by accepting void pointers and dereferencing them to obtain QTNode pointers before delegating the actual comparison to QTNodeCompare.

The function performs the necessary pointer casting and dereferencing to convert from the generic void pointer interface required by qsort to the specific QTNode pointer interface used by QTNodeCompare. This abstraction allows QTNode arrays to be sorted using standard library sorting functions.

## Parameters / Member Variables
- : Pointer to the first QTNode pointer (cast from void*)
- : Pointer to the second QTNode pointer (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - [QTNodeCompare](../Q/QTNodeCompare.md) (actual comparison implementation)
- Data types used:
  - [QTNode](../Q/QTNode.md) (through pointer casting)
- Called from (representative examples):
  - [QTNSort](../Q/QTNSort.md)

## Notes and Other Information
- Static function, only accessible within the tsquery_util.c compilation unit
- Essential bridge between C standard library qsort interface and PostgreSQL's QTNode comparison logic
- Performs double pointer dereferencing:  to extract QTNode from void pointer to pointer
- Returns same comparison values as QTNodeCompare (-1, 0, 1) for sort compatibility
- Used exclusively by QTNSort for array sorting operations