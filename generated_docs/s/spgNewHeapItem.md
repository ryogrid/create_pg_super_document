# spgNewHeapItem

## Location
[src/backend/access/spgist/spgscan.c:463-515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L463-L515)

## Overview
Creates a new SpGistSearchItem for a leaf tuple in an SP-GiST index scan, storing all necessary information for heap tuple retrieval and result reconstruction.

## Definition

```c
structed value, copy it to queue cxt out of tmp
	 * cxt.  Caution: the leaf_consistent method may not have supplied a value
	 * if we didn't ask it to, and mildly-broken methods might supply one of
	 * the wrong type.  The correct leafValue type is attType not leafType.
	 */
	if (so->want_itup)
	{
		item->value = isnull ? (Datum) 0 :
			datumCopy(leafValue, so->state.attType.attbyval,
					  so->state.attType.attlen);

		/*
		 * If we're going to need to reconstruct INCLUDE attributes, store the
		 * whole leaf tuple so we can get the INCLUDE attributes out of it.
		 */
		if (so->state.leafTupDesc->natts > 1)
		{
			item->leafTuple = palloc(leafTuple->size);
			memcpy(item->leafTuple, leafTuple, leafTuple->size);
		}
		else
			item->leafTuple = NULL;
	}
	else
	{
		item->value = (Datum) 0;
		item->leafTuple = NULL;
	}
	item->traversalValue = NULL;
```
## Detailed Description
This function constructs a SpGistSearchItem specifically for leaf tuples during SP-GiST index scanning. It serves as a constructor for search items that represent actual heap tuples found during the scan. The function handles the proper copying of leaf values and tuple data from temporary memory contexts to the queue context for safe storage throughout the scan process.

The function is responsible for:
- Allocating a new search item in the queue context
- Setting up the heap pointer for tuple retrieval
- Copying the reconstructed value if needed (handling type safety)
- Storing the complete leaf tuple if INCLUDE attributes are present
- Setting flags for result processing (recheck, distance rechecking)

## Parameters
- : SpGistScanOpaque - The scan state containing configuration and context information
- : int - The tree level where this leaf was found
- : SpGistLeafTuple - The leaf tuple containing the heap pointer and data
- : Datum - The reconstructed value from the leaf_consistent method
- : bool - Whether the result needs to be rechecked against the original condition
- : bool - Whether distance calculations need to be rechecked
- : bool - Whether the leaf value is NULL
- : double * - Array of distance values for ordering results

## Dependencies
- Functions called/Symbols referenced:
  - [spgAllocSearchItem](spgAllocSearchItem.md) - Allocates a new search item in queue context
  - [datumCopy](../d/datumCopy.md) - Creates a proper copy of the datum value
  - [palloc](../p/palloc.md) - Allocates memory for leaf tuple storage
  - memcpy - Copies leaf tuple data
- Called from:
  - [spgLeafTest](spgLeafTest.md) - Creates heap items for qualifying leaf tuples

## Notes and Other Information
- The function carefully handles memory context issues by copying data from temporary context to queue context
- It includes type safety checks, noting that leaf_consistent methods might supply wrong-typed values
- The function handles INCLUDE attributes by storing the complete leaf tuple when multiple attributes are present
- All search items created are marked as leaf items (isLeaf = true)
- The traversalValue is always set to NULL for leaf items since no further traversal is needed