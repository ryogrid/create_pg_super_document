# PrepareSortSupportFromOrderingOp

## Location
[src/backend/utils/sort/sortsupport.c:134-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sortsupport.c#L134-L160)

## Overview
Sets up a SortSupport structure using a btree ordering operator ("<" or ">" operator) to determine the appropriate comparison function and sort direction.

## Definition

```c
void
PrepareSortSupportFromOrderingOp(Oid orderingOp, SortSupport ssup)
```
## Detailed Description
PrepareSortSupportFromOrderingOp is a public interface function that configures sort support functionality based on a PostgreSQL ordering operator. The function:

1. **Validates the operator**: Uses get_ordering_op_properties to look up the operator in pg_amop and verify it's a valid ordering operator
2. **Determines sort direction**: Sets ssup_reverse based on whether the operator is a "greater than" operator (BTGreaterStrategyNumber)
3. **Configures the comparator**: Delegates to FinishSortSupportFunction to set up the actual comparison function

This function serves as a bridge between PostgreSQL's operator system and the SortSupport framework, allowing sorts to be configured using familiar SQL operators rather than requiring direct knowledge of operator families and comparison functions.

The caller must pre-initialize the SortSupport structure by zeroing it and setting ssup_cxt (memory context), ssup_collation, and ssup_nulls_first before calling this function.

## Parameters / Member Variables
- : OID of the btree ordering operator ("<" or ">" operator)
- : SortSupport structure to be configured (must be pre-initialized)

## Dependencies
- Functions called/Symbols referenced:
  - [SortSupport](../S/SortSupport.md) (type)
  - [get_ordering_op_properties](../g/get_ordering_op_properties.md)
  - BTGreaterStrategyNumber
  - [FinishSortSupportFunction](../F/FinishSortSupportFunction.md)
- Called from:
  - [compute_scalar_stats](../c/compute_scalar_stats.md) (at src/backend/commands/analyze.c:2397)
  - [ExecInitGatherMerge](../E/ExecInitGatherMerge.md) (at src/backend/executor/nodeGatherMerge.c:165)
  - [ExecInitIndexScan](../E/ExecInitIndexScan.md) (at src/backend/executor/nodeIndexscan.c:1030)
  - [ExecInitMergeAppend](../E/ExecInitMergeAppend.md) (at src/backend/executor/nodeMergeAppend.c:182)
  - [multi_sort_add_dimension](../m/multi_sort_add_dimension.md) (at src/backend/statistics/extended_stats.c:860)
  - [statext_mcv_serialize](../s/statext_mcv_serialize.md) (at src/backend/statistics/mcv.c:693)
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md) (at src/backend/utils/sort/tuplesortvariants.c:225)
  - [tuplesort_begin_datum](../t/tuplesort_begin_datum.md) (at src/backend/utils/sort/tuplesortvariants.c:647)
  - [ApplySortAbbrevFullComparator](../A/ApplySortAbbrevFullComparator.md) (at src/include/utils/sortsupport.h:386)

## Notes and Other Information
- This is a public function, part of PostgreSQL's sort support API
- Widely used throughout the executor, statistics, and utility modules
- The function assumes the SortSupport structure has been properly pre-initialized by the caller
- Provides a user-friendly interface for setting up sorts based on SQL operators
- Automatically handles the complexity of mapping operators to operator families and comparison functions
- Essential for operations like ORDER BY clauses, merge joins, and various sorting algorithms throughout PostgreSQL