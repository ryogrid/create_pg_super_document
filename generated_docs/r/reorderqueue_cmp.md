# reorderqueue_cmp

## Location
[src/backend/executor/nodeIndexscan.c:441-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L441-L457)

## Overview
A comparison function used by the pairing heap to order tuples in a reorder queue during K-nearest neighbor (KNN) index scans, inverting the sort order to achieve ascending results.

## Definition
```c
static int reorderqueue_cmp(const pairingheap_node *a, const pairingheap_node *b, void *arg)
```

## Detailed Description
This function serves as a comparison callback for a pairing heap data structure used in PostgreSQL's index scanning operations. The pairing heap naturally provides the greatest element at the top, but KNN operations require ascending order results. To achieve this, reorderqueue_cmp inverts the comparison by swapping the argument order when calling the underlying comparison function cmp_orderbyvals.

The function extracts ReorderTuple structures from the pairing heap nodes and compares their order-by values. By reversing the argument order in the comparison, it effectively inverts the heap's natural descending order to provide ascending order for KNN results.

## Parameters / Member Variables
- `a`: First pairing heap node containing a ReorderTuple to compare
- `b`: Second pairing heap node containing a ReorderTuple to compare  
- `arg`: Pointer to IndexScanState containing context information for the comparison

## Dependencies
- Functions called/Symbols referenced:
  - [cmp_orderbyvals](../c/cmp_orderbyvals.md)
  - ReorderTuple (struct)
  - [IndexScanState](../I/IndexScanState.md) (struct)
  - [pairingheap_node](../p/pairingheap_node.md) (struct)
- Called from (representative examples):
  - [ExecInitIndexScan](../E/ExecInitIndexScan.md) (during pairing heap initialization)

## Notes and Other Information
- This function is specifically designed for KNN (K-nearest neighbor) index scans
- The comment explicitly explains the rationale for inverting the sort order
- The function is static, indicating it's only used within the nodeIndexscan.c file
- Part of PostgreSQL's executor subsystem for efficient ordered result retrieval