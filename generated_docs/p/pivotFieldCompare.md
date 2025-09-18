# pivotFieldCompare

## Location
[src/bin/psql/crosstabview.c:695-710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L695-L710)

## Overview
A comparison function for pivot_field structures that compares their name values, treating null values as equal and ordering non-null values lexicographically.

## Definition


## Detailed Description
This function implements a three-way comparison for pivot_field structures used in PostgreSQL's psql \crosstabview feature. It serves as a comparator for deduplication and sorting operations. The comparison logic follows a specific hierarchy: null values are considered equal to each other, non-null values are always considered less than null values, and non-null values are compared lexicographically using strcmp().

The function is designed to be used with standard library functions like qsort() and in AVL tree operations, providing a consistent ordering that enables efficient deduplication of pivot field values in crosstab reports.

## Parameters / Member Variables
- `a`: Pointer to the first pivot_field structure to compare
- `b`: Pointer to the second pivot_field structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard string comparison function)
- Called from (representative examples):
  - [printCrosstab](printCrosstab.md) (for sorting pivot field arrays)
  - [avlInsertNode](../a/avlInsertNode.md) (for maintaining AVL tree ordering)

## Notes and Other Information
- Returns 0 if both names are equal (including both being null)
- Returns -1 if first name is non-null and second is null, or if first name is lexicographically less
- Returns 1 if first name is null and second is non-null, or if first name is lexicographically greater
- Specifically designed for deduplication purposes in crosstab processing
- Null values are treated as the 'largest' values in the ordering (non-null < null)
- Compatible with standard library qsort() and other comparison-based algorithms
- Used internally by AVL tree operations to maintain proper ordering of pivot field values