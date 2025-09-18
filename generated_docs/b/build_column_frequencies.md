# build_column_frequencies

## Location
[src/backend/statistics/mcv.c:490-557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L490-L557)

## Overview
Computes frequencies of individual values in each column to support base frequency calculation for MCV (Most Common Values) statistics.

## Definition
```c
static SortItem **build_column_frequencies(SortItem *groups, int ngroups, MultiSortSupport mss, int *ncounts)
```

## Detailed Description
This function analyzes distinct value groups and computes frequency counts for individual column values across all dimensions. It creates arrays of SortItems for each attribute, where each SortItem represents a unique value with its total frequency count. The function optimizes memory usage by allocating all arrays in a single chunk and reusing value/isnull pointers from the input groups. For each column, it sorts values, identifies duplicates, and sums their frequencies to produce accurate per-value statistics used in MCV base frequency calculations.

## Parameters / Member Variables
- `groups`: Array of distinct value groups with their counts
- `ngroups`: Number of distinct groups in the input array
- `mss`: MultiSortSupport structure containing sort specifications for all columns
- `ncounts`: Output array that receives the count of distinct values for each column

## Dependencies
- Functions called/Symbols referenced:
  - [sort_item_compare](../s/sort_item_compare.md)
  - qsort_interruptible
  - [palloc](../p/palloc.md)
  - MAXALIGN (macro)
- Called from (representative examples):
  - SizeOfMCVList
  - [statext_mcv_build](../s/statext_mcv_build.md)

## Notes and Other Information
- Allocates all memory in a single chunk for efficient memory management
- Reuses value/isnull pointers from input groups to avoid data duplication
- Processes each column dimension independently for multi-column statistics
- Sorts and deduplicates values to compute accurate frequency counts
- Essential component for calculating base frequencies in MCV list generation
- Memory can be freed with a single pfree call due to chunk allocation strategy