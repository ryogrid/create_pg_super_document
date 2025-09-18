# make_incrementalsort

## Location
src/backend/optimizer/plan/createplan.c: 6099 - 6164

## Overview
The make_incrementalsort function creates an IncrementalSort plan node, which performs sorting optimized for cases where the input is already partially sorted according to some prefix of the sort columns.

## Definition
```c
static IncrementalSort *
make_incrementalsort(Plan *lefttree, int numCols, int nPresortedCols,
                     AttrNumber *sortColIdx, Oid *sortOperators,
                     Oid *collations, bool *nullsFirst)
```

## Detailed Description
The make_incrementalsort function constructs an IncrementalSort plan node that implements an optimized sorting algorithm for scenarios where the input data is already sorted by some prefix of the desired sort columns. This optimization allows PostgreSQL to avoid re-sorting the already-sorted prefix columns and only sort within groups that have the same values for those prefix columns.

The IncrementalSort node extends the basic Sort functionality by tracking how many columns are already presorted (nPresortedCols), enabling more efficient execution when the input has partial ordering. The function initializes the embedded Sort structure within the IncrementalSort node and sets up the presorted column count.

## Parameters / Member Variables
- `lefttree`: The child plan node that provides the input tuples to be incrementally sorted
- `numCols`: The total number of columns to sort by
- `nPresortedCols`: The number of leading columns that are already sorted in the input
- `sortColIdx`: Array of column indices (attribute numbers) to sort by
- `sortOperators`: Array of OIDs for the sorting operators to use for each column
- `collations`: Array of OIDs for the collation rules to apply to each sorting column
- `nullsFirst`: Array of boolean flags indicating whether NULLs should sort before non-NULLs for each column

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the IncrementalSort node)
  - IncrementalSort (plan node type)
- Called from (representative examples):
  - [make_incrementalsort_from_pathkeys](make_incrementalsort_from_pathkeys.md) (src/backend/optimizer/plan/createplan.c:6403)

## Notes and Other Information
- This is a static function within createplan.c, used internally by the planner
- IncrementalSort is an optimization introduced in PostgreSQL 13 for handling partially sorted input
- The nPresortedCols parameter must be less than numCols and indicates how many leading columns are already in sorted order
- The function accesses the Sort structure embedded within IncrementalSort via node->sort
- Like make_sort, this function assumes the caller has prepared all sorting arrays correctly
- The IncrementalSort algorithm can significantly improve performance when input data has natural ordering
- Located at src/backend/optimizer/plan/createplan.c:6099-6164