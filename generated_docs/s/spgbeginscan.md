# spgbeginscan

## Location
[src/backend/access/spgist/spgscan.c:304-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L304-L379)

## Overview
Initializes and begins a new SP-GiST index scan, setting up all necessary data structures and memory contexts for scanning operations.

## Definition
```c
IndexScanDesc spgbeginscan(Relation rel, int keysz, int orderbysz)
```

## Detailed Description
This function creates and initializes a new SP-GiST index scan descriptor. It allocates memory for the SpGistScanOpaque structure and sets up all necessary components including memory contexts for temporary storage and traversal values, tuple descriptors for index-only scans, and arrays for order-by operations.

The function also prepares function manager info for the inner and leaf consistent functions that will be used during scanning, and handles the setup of distance arrays for nearest-neighbor searches when order-by clauses are present.

## Parameters / Member Variables
- `rel`: The relation (index) being scanned
- `keysz`: Number of scan keys (search conditions)
- `orderbysz`: Number of order-by expressions for distance-ordered scans

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexScan](../R/RelationGetIndexScan.md)
  - [palloc0](../p/palloc0.md)/palloc
  - [initSpGistState](../i/initSpGistState.md)
  - AllocSetContextCreate
  - [getSpGistTupleDesc](../g/getSpGistTupleDesc.md)
  - [get_float8_infinity](../g/get_float8_infinity.md)
  - [fmgr_info_copy](../f/fmgr_info_copy.md)
  - [index_getprocinfo](../i/index_getprocinfo.md)
  - SPGIST_INNER_CONSISTENT_PROC (constant)
  - SPGIST_LEAF_CONSISTENT_PROC (constant)
  - ALLOCSET_DEFAULT_SIZES (constant)
- Called from:
  - [spghandler](spghandler.md) (src/backend/access/spgist/spgutils.c:84)

## Dependencies
- Types used:
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - SpGistScanOpaque
  - [SpGistScanOpaqueData](../S/SpGistScanOpaqueData.md)
  - ScanKey
  - [Relation](../R/Relation.md)

## Notes and Other Information
- Creates two separate memory contexts: one for temporary search operations and another for traversal values
- Sets up tuple descriptor for potential index-only scans using getSpGistTupleDesc
- For distance-ordered scans, allocates and initializes arrays for zero distances and infinite distances
- Copies function manager information for inner and leaf consistent procedures from the index's operator class
- The returned IndexScanDesc needs to be used with spgrescan before actual scanning can begin
- Memory allocation uses CurrentMemoryContext as the parent for the created contexts

## Simplified Source

```c
IndexScanDesc
spgbeginscan(Relation rel, int keysz, int orderbysz)
{
    // Get basic index scan descriptor
    IndexScanDesc scan = RelationGetIndexScan(rel, keysz, orderbysz);

    // Allocate and initialize scan opaque structure
    SpGistScanOpaque so = palloc0(sizeof(SpGistScanOpaqueData));

    // Setup scan key storage
    if (keysz > 0)
        so->keyData = palloc(sizeof(ScanKeyData) * keysz);
    else
        so->keyData = NULL;

    // Initialize SP-GiST state
    initSpGistState(&so->state, scan->indexRelation);

    // Create memory contexts for search operations
    so->tempCxt = AllocSetContextCreate(CurrentMemoryContext,
                                        "SP-GiST search temporary context",
                                        ALLOCSET_DEFAULT_SIZES);
    so->traversalCxt = AllocSetContextCreate(CurrentMemoryContext,
                                             "SP-GiST traversal-value context",
                                             ALLOCSET_DEFAULT_SIZES);

    // Setup tuple descriptor for index-only scans
    so->reconTupDesc = scan->xs_hitupdesc =
        getSpGistTupleDesc(rel, &so->state.attType);

    // Setup arrays for distance-ordered scans
    if (scan->numberOfOrderBys > 0) {
        so->orderByTypes = palloc(sizeof(Oid) * scan->numberOfOrderBys);
        so->nonNullOrderByOffsets = palloc(sizeof(int) * scan->numberOfOrderBys);
        so->zeroDistances = palloc(sizeof(double) * scan->numberOfOrderBys);
        so->infDistances = palloc(sizeof(double) * scan->numberOfOrderBys);

        // Initialize distance arrays
        for (int i = 0; i < scan->numberOfOrderBys; i++) {
            so->zeroDistances[i] = 0.0;
            so->infDistances[i] = get_float8_infinity();
        }

        // Allocate order-by result arrays
        scan->xs_orderbyvals = palloc0(sizeof(Datum) * scan->numberOfOrderBys);
        scan->xs_orderbynulls = palloc(sizeof(bool) * scan->numberOfOrderBys);
        memset(scan->xs_orderbynulls, true, sizeof(bool) * scan->numberOfOrderBys);
    }

    // Copy function info for consistent procedures
    fmgr_info_copy(&so->innerConsistentFn,
                   index_getprocinfo(rel, 1, SPGIST_INNER_CONSISTENT_PROC),
                   CurrentMemoryContext);
    fmgr_info_copy(&so->leafConsistentFn,
                   index_getprocinfo(rel, 1, SPGIST_LEAF_CONSISTENT_PROC),
                   CurrentMemoryContext);

    so->indexCollation = rel->rd_indcollation[0];
    scan->opaque = so;

    return scan;
}
```