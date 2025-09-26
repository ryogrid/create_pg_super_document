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