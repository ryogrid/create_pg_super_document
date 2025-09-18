# ApplySortAbbrevFullComparator

## Location
[src/include/utils/sortsupport.h:341-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/sortsupport.h#L341-L391)

## Overview
ApplySortAbbrevFullComparator is an inline function that applies a sort comparator using the full, authoritative comparator function, handling reverse-sort and NULL ordering properly.

## Definition


## Detailed Description
This function provides a comparison interface specifically designed for abbreviated sorting scenarios where a full comparison is needed. Unlike the basic ApplySortComparator which uses the regular comparator function pointer, this function uses the abbrev_full_comparator function pointer from the SortSupport structure.

The function is typically used in tiebreaker scenarios during abbreviated sorting, where an abbreviated comparison was inconclusive and a full authoritative comparison is required to determine the final ordering. It implements the same NULL handling and sort direction logic as other comparator functions.

## Parameters / Member Variables
- : The first Datum value to compare
- : Boolean flag indicating whether datum1 is NULL
- : The second Datum value to compare
- : Boolean flag indicating whether datum2 is NULL
- : SortSupport structure containing the abbrev_full_comparator function and sort configuration

## Dependencies
- Functions called/Symbols referenced:
  - SortSupport (struct type)
  - ssup->abbrev_full_comparator (function pointer)
  - INVERT_COMPARE_RESULT (macro)
  - Various utility functions: ssup_datum_unsigned_cmp, ssup_datum_signed_cmp, ssup_datum_int32_cmp
  - Preparation functions: PrepareSortSupportComparisonShim, PrepareSortSupportFromOrderingOp, PrepareSortSupportFromIndexRel, PrepareSortSupportFromGistIndexRel
- Called from (representative examples):
  - [comparetup_heap_tiebreak](../c/comparetup_heap_tiebreak.md) (src/backend/utils/sort/tuplesortvariants.c:1132)
  - comparetup_cluster_tiebreak (src/backend/utils/sort/tuplesortvariants.c:1278)
  - [comparetup_index_btree_tiebreak](../c/comparetup_index_btree_tiebreak.md) (src/backend/utils/sort/tuplesortvariants.c:1494)
  - comparetup_datum_tiebreak (src/backend/utils/sort/tuplesortvariants.c:1816)

## Notes and Other Information
This function is a critical component of PostgreSQL's abbreviated sorting optimization. Abbreviated sorting uses shorter representations of values for initial comparisons to improve performance, but when these abbreviated comparisons are inconclusive (i.e., the abbreviated keys are equal but the full values might not be), this function provides the definitive comparison using the full authoritative comparator. The function is primarily used in tiebreaker scenarios across various tuple sorting variants to ensure correct ordering when abbreviated comparisons are insufficient.