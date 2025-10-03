# multi_sort_add_dimension

## Location
[src/backend/statistics/extended_stats.c:851-864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L851-L864)

## Overview
Prepares sort support information for multi-column sorting operations by configuring a specific dimension within a MultiSortSupport structure using the provided sort operator and collation.

## Definition

```c
void
multi_sort_add_dimension(MultiSortSupport mss, int sortdim,
						 Oid oper, Oid collation)
```
## Detailed Description
This function initializes a single dimension of a multi-dimensional sort operation by setting up the SortSupport structure at the specified dimension index. It configures the sort support with the current memory context, the specified collation, and prepares the actual sort comparison function using the provided ordering operator. The function is primarily used in PostgreSQL's extended statistics subsystem to enable efficient multi-column sorting operations for statistical calculations.

## Parameters / Member Variables
- `mss`: MultiSortSupport structure containing an array of SortSupport elements for multi-dimensional sorting
- `sortdim`: Integer index specifying which dimension (column) in the sort operation to configure
- `oper`: OID of the ordering operator to use for comparisons in this dimension
- `collation`: OID of the collation to apply for text comparisons in this dimension
## Dependencies
- Functions called/Symbols referenced:
  - [PrepareSortSupportFromOrderingOp](../P/PrepareSortSupportFromOrderingOp.md)
  - MultiSortSupport (type)
  - [SortSupport](../S/SortSupport.md) (type)
- Called from (representative examples):
  - [dependency_degree](../d/dependency_degree.md) (src/backend/statistics/dependencies.c:275)
  - [build_mss](../b/build_mss.md) (src/backend/statistics/mcv.c:366)
  - [ndistinct_for_combination](../n/ndistinct_for_combination.md) (src/backend/statistics/mvdistinct.c:480)

## Notes and Other Information
- Sets ssup_nulls_first to false, indicating that NULL values are sorted after non-NULL values
- Uses CurrentMemoryContext for memory allocation context
- Part of PostgreSQL's extended statistics infrastructure used for multi-variate statistical analysis
- The function assumes the MultiSortSupport structure has been properly allocated with sufficient dimensions

## Simplified Source

```c
void
multi_sort_add_dimension(MultiSortSupport mss, int sortdim,
                         Oid oper, Oid collation)
{
    SortSupport ssup = &mss->ssup[sortdim];

    // Configure sort support for this dimension
    ssup->ssup_cxt = CurrentMemoryContext;
    ssup->ssup_collation = collation;
    ssup->ssup_nulls_first = false;

    // Prepare the comparison function for the ordering operator
    PrepareSortSupportFromOrderingOp(oper, ssup);
}
```