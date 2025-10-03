# tuplesort_begin_cluster

## Location
[src/backend/utils/sort/tuplesortvariants.c:243-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L243-L351)

## Overview
Initializes a Tuplesortstate for cluster operations that sort heap tuples according to a B-tree index ordering, used primarily for the CLUSTER command to reorganize table data.

## Definition

```c
Tuplesortstate *
tuplesort_begin_cluster(TupleDesc tupDesc,
						Relation indexRel,
						int workMem,
						SortCoordinate coordinate, int sortopt)
```
## Detailed Description
This function creates a specialized tuplesort state for cluster operations, which sort heap tuples according to the ordering defined by a B-tree index. It builds the necessary infrastructure to compare tuples based on the index's key attributes, handling both simple column references and complex index expressions. The function sets up execution state and expression evaluation context when the index contains expressions, ensuring proper tuple comparison during the clustering process. It configures sort support for each index key attribute, respecting the index's collation, null ordering, and sort direction.

## Parameters / Member Variables
- `tupDesc`: Tuple descriptor for the heap tuples being sorted
- `indexRel`: B-tree index relation that defines the sort ordering (must be BTREE_AM_OID)
- `workMem`: Amount of memory (in KB) available for sorting operations
- `coordinate`: Coordination structure for parallel sorting operations
- `sortopt`: Sorting options bitmask (e.g., TUPLESORT_RANDOMACCESS)
## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_begin_common](tuplesort_begin_common.md)
  - TuplesortstateGetPublic
  - IndexRelationGetNumberOfKeyAttributes
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [_bt_mkscankey](../b/_bt_mkscankey.md)
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - GetPerTupleExprContext
  - [removeabbrev_cluster](../r/removeabbrev_cluster.md)
  - [comparetup_cluster](../c/comparetup_cluster.md)
  - [comparetup_cluster_tiebreak](../c/comparetup_cluster_tiebreak.md)
  - [writetup_cluster](../w/writetup_cluster.md)
  - [readtup_cluster](../r/readtup_cluster.md)
  - [freestate_cluster](../f/freestate_cluster.md)
  - [PrepareSortSupportFromIndexRel](../P/PrepareSortSupportFromIndexRel.md)
- Called from (representative examples):
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md) (heapam_handler.c:731)

## Notes and Other Information
- Requires the index relation to be a B-tree index (asserts BTREE_AM_OID)
- Handles complex index expressions by setting up an executor state and expression context
- Disables datum1 optimization when the leading attribute is an expression (ii_IndexAttrNumbers[0] == 0)
- Respects index-specific sort properties like DESC ordering and NULLS FIRST/LAST
- Creates a TuplesortClusterArg structure to store cluster-specific arguments including IndexInfo and TupleDesc
- The function assumes TupleDesc doesn't need copying and stores it directly
- Uses the index's scan key information to configure sort support for each key attribute