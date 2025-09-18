# compare_datums_simple

## Location
[src/backend/statistics/extended_stats.c:927-940](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L927-L940)

## Overview
A simple comparison function for datums that applies sort comparison logic without null handling.

## Definition


## Detailed Description
This function serves as a wrapper around PostgreSQL's ApplySortComparator function, providing a simplified interface for comparing two Datum values. It assumes that neither datum is null and directly applies the sort comparison logic using the provided SortSupport structure. This function is typically used in scenarios where null values have already been filtered out or handled separately, allowing for straightforward datum comparison.

## Parameters / Member Variables
- : The first Datum value to compare
- : The second Datum value to compare  
- : SortSupport structure containing comparison function and collation information

## Dependencies
- Functions called/Symbols referenced:
  - [ApplySortComparator](../A/ApplySortComparator.md)
  - SortSupport (type)
- Called from (representative examples):
  - [compare_scalars_simple](compare_scalars_simple.md)
  - [statext_mcv_serialize](../s/statext_mcv_serialize.md)

## Notes and Other Information
- This function assumes both input datums are non-null, as indicated by the 'false' parameters passed to ApplySortComparator
- Used primarily in extended statistics processing where simplified comparison is needed
- Returns standard comparison result: negative for a < b, zero for a = b, positive for a > b
- Located in src/backend/statistics/extended_stats.c:927-940