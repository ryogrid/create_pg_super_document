# storeGettuple

## Location
[src/backend/access/spgist/spgscan.c:959-1025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L959-L1025)

## Overview
Helper function for SPGiST index scanning that stores a retrieved heap tuple pointer and associated metadata during gettuple operations.

## Definition

```c
struct index data.  We have to copy the datum out of the temp
		 * context anyway, so we may as well create the tuple here.
		 */
		Datum		leafDatums[INDEX_MAX_KEYS];
```
## Detailed Description
This function is a subroutine used during SPGiST index scans to store information about a retrieved tuple. It manages the storage of heap tuple pointers, recheck flags, distance calculations for ORDER BY operations, and reconstructed index tuples when needed. The function operates within the context of SPGiST (Space-Partitioned Generalized Search Tree) index scanning and is specifically designed for gettuple-style operations where individual tuples are retrieved one at a time.

The function handles distance calculations for nearest-neighbor searches and can reconstruct index tuples when the scan requires them. It ensures proper memory management and data organization within the scan opaque structure.

## Parameters / Member Variables
- : SPGiST scan opaque structure containing scan state and result arrays
- : Pointer to the heap tuple being stored
- : The key value from the leaf tuple
- : Flag indicating whether the leaf value is NULL
- : The complete leaf tuple from the index
- : Flag indicating whether the tuple needs to be rechecked against scan conditions
- : Flag indicating whether distance calculations need rechecking
- : Array of non-NULL distance values for ORDER BY operations

## Dependencies
- Functions called/Symbols referenced:
  - [spgDeformLeafTuple](spgDeformLeafTuple.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - pPalloc (implicit through palloc)
- Types used:
  - SpGistScanOpaque
  - [SpGistLeafTuple](../S/SpGistLeafTuple.md)
  - [IndexOrderByDistance](../I/IndexOrderByDistance.md)
  - ItemPointer
- Constants:
  - MaxIndexTuplesPerPage
  - INDEX_MAX_KEYS
  - spgKeyColumn
- Called from:
  - [spggettuple](spggettuple.md)

## Notes and Other Information
- This is a static helper function specific to the SPGiST scan implementation
- Handles both simple scans and complex ORDER BY operations with distance calculations
- Manages memory allocation for distance arrays when ORDER BY clauses are present
- Can reconstruct index tuples when the scan operation requires complete tuple information
- Maintains arrays in the scan opaque structure for batch processing of results
- Part of the SPGiST access method implementation for PostgreSQL's indexing system

## Simplified Source

```c
static void
storeGettuple(SpGistScanOpaque so, ItemPointer heapPtr,
              Datum leafValue, bool isnull,
              SpGistLeafTuple leafTuple, bool recheck,
              bool recheckDistances, double *nonNullDistances)
{
    // Store basic tuple information
    so->heapPtrs[so->nPtrs] = *heapPtr;
    so->recheck[so->nPtrs] = recheck;
    so->recheckDistances[so->nPtrs] = recheckDistances;

    // Handle ORDER BY distance calculations
    if (so->numberOfOrderBys > 0) {
        if (isnull || so->numberOfNonNullOrderBys <= 0) {
            so->distances[so->nPtrs] = NULL;
        } else {
            // Allocate and populate distance array
            IndexOrderByDistance *distances =
                palloc(sizeof(distances[0]) * so->numberOfOrderBys);

            for (int i = 0; i < so->numberOfOrderBys; i++) {
                int offset = so->nonNullOrderByOffsets[i];

                if (offset >= 0) {
                    // Copy non-NULL distance value
                    distances[i].value = nonNullDistances[offset];
                    distances[i].isnull = false;
                } else {
                    // Set NULL distance
                    distances[i].value = 0.0;
                    distances[i].isnull = true;
                }
            }

            so->distances[so->nPtrs] = distances;
        }
    }

    // Reconstruct index tuple if needed
    if (so->want_itup) {
        Datum leafDatums[INDEX_MAX_KEYS];
        bool leafIsnulls[INDEX_MAX_KEYS];

        // Handle INCLUDE attributes if present
        if (so->state.leafTupDesc->natts > 1)
            spgDeformLeafTuple(leafTuple, so->state.leafTupDesc,
                              leafDatums, leafIsnulls, isnull);

        // Set key column value
        leafDatums[spgKeyColumn] = leafValue;
        leafIsnulls[spgKeyColumn] = isnull;

        // Form reconstructed tuple
        so->reconTups[so->nPtrs] = heap_form_tuple(so->reconTupDesc,
                                                  leafDatums, leafIsnulls);
    }

    so->nPtrs++;
}
```