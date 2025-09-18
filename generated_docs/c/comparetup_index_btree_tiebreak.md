# comparetup_index_btree_tiebreak

## Location
[src/backend/utils/sort/tuplesortvariants.c:1466-1587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1466-L1587)

## Overview
A specialized comparison function for B-tree index sorting that performs tiebreaking comparisons when the primary sort keys are equal, including uniqueness enforcement and ItemPointer-based ordering.

## Definition


## Detailed Description
This function serves as a tiebreaker comparison routine for B-tree index tuple sorting. It performs a comprehensive comparison when initial sort keys are equal, handling:

1. **Abbreviated key comparison**: If an abbreviation converter is available, it performs a full comparison on the first key using the abbreviated comparator
2. **Multi-key comparison**: Iterates through all sort keys (starting from key 2) to find differences
3. **Uniqueness enforcement**: When uniqueness is required, detects and reports duplicate key violations (respecting NULL handling rules)
4. **ItemPointer tiebreaking**: Uses heap TID (tuple identifier) as the final comparison criterion to ensure deterministic ordering

The function ensures that B-tree indexes maintain their required physical uniqueness property by treating heap TID as an implicit last key attribute.

## Parameters / Member Variables
- : First SortTuple to compare containing an IndexTuple
- : Second SortTuple to compare containing an IndexTuple  
- : Tuplesortstate containing sort configuration and context information

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [index_getattr](../i/index_getattr.md)
  - [ApplySortAbbrevFullComparator](../A/ApplySortAbbrevFullComparator.md)
  - [ApplySortComparator](../A/ApplySortComparator.md)
  - [index_deform_tuple](../i/index_deform_tuple.md)
  - [BuildIndexValueDescription](../B/BuildIndexValueDescription.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [errtableconstraint](../e/errtableconstraint.md)
- Called from (representative examples):
  - [tuplesort_begin_index_btree](../t/tuplesort_begin_index_btree.md)
  - [tuplesort_begin_index_gist](../t/tuplesort_begin_index_gist.md)
  - [comparetup_index_btree](comparetup_index_btree.md)
  - CLUSTER_SORT

## Notes and Other Information
- The function assumes that primary key comparison has already been performed and found the tuples to be equal
- Uniqueness violations are only reported when enforceUnique is true and appropriate NULL handling rules are met
- The final ItemPointer comparison should never result in equality for valid tuples, hence the Assert(false) at the end
- This function is critical for maintaining B-tree index integrity and ensuring deterministic sort order
- NULL values in keys are tracked to properly handle uniqueness constraints with NULLS NOT DISTINCT semantics