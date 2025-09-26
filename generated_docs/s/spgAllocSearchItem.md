# spgAllocSearchItem

## Location
[src/backend/access/spgist/spgscan.c:114-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L114-L129)

## Overview
A memory allocation function that creates and initializes a new SpGistSearchItem structure with appropriate sizing for distance arrays based on the number of ORDER BY clauses in KNN searches.

## Definition

```c
static SpGistSearchItem *
spgAllocSearchItem(SpGistScanOpaque so, bool isnull, double *distances)
```
## Detailed Description
This function handles the dynamic allocation of SpGistSearchItem structures, implementing intelligent memory management by only allocating space for distance arrays when needed. The function uses the SizeOfSpGistSearchItem macro to calculate the appropriate structure size based on whether the item is NULL and how many ORDER BY clauses require distance tracking.

For non-NULL items with distance-based ordering, the function allocates additional space for the distances array and copies the provided distance values. This design optimizes memory usage by avoiding unnecessary distance array allocation for NULL items or scans without ORDER BY clauses.

## Parameters / Member Variables
- : SpGistScanOpaque structure containing scan context, specifically the numberOfNonNullOrderBys field used for sizing calculations
- : Boolean flag indicating whether this search item represents a NULL value (affects memory allocation size)
- : Pointer to array of double values representing distances from ORDER BY clauses (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - memcpy (standard library function for memory copying)
  - SizeOfSpGistSearchItem (macro calculating structure size based on distance array needs)
  - SpGistScanOpaque (scan context structure type)
  - [SpGistSearchItem](../S/SpGistSearchItem.md) (search item structure type)
- Called from (representative examples):
  - [spgAddStartItem](spgAddStartItem.md) (allocates initial search items)
  - [spgNewHeapItem](spgNewHeapItem.md) (creates items for heap tuples)
  - [spgMakeInnerItem](spgMakeInnerItem.md) (creates items for inner nodes)

## Notes and Other Information
- Uses variable-sized allocation based on the number of ORDER BY clauses to optimize memory usage
- NULL items get minimal allocation (no space for distances array)
- Distance array is only populated if the item is non-NULL and there are ORDER BY clauses
- Part of the memory management infrastructure for SP-GiST KNN search operations
- The returned item must eventually be freed using spgFreeSearchItem to prevent memory leaks