# bms_compare

## Location
[src/backend/nodes/bitmapset.c:183-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L183-L215)

## Overview
A qsort-style comparator function for Bitmapsets that provides consistent ordering based on the highest-numbered differing bit.

## Definition
int bms_compare(const Bitmapset *a, const Bitmapset *b)

## Detailed Description
bms_compare implements a lexicographic comparison algorithm for Bitmapsets, designed to be compatible with standard sorting functions like qsort. The function establishes a total ordering over all possible Bitmapsets using the following precedence rules:

1. **NULL handling**: NULL (empty set) is considered less than any non-empty set, with two NULLs being equal
2. **Word count comparison**: Sets with more words are considered greater, as they contain higher-numbered bits
3. **Lexicographic comparison**: Starting from the highest word, the first differing word determines the result

The comparison algorithm ensures that bms_compare returns 0 if and only if bms_equal would return true for the same inputs. For unequal sets, the result is determined by the highest-numbered bit position where the sets differ, making the comparison intuitive (e.g., {6} > {5}).

## Parameters / Member Variables
- `a`: A constant pointer to the first Bitmapset to compare. Can be NULL.
- `b`: A constant pointer to the second Bitmapset to compare. Can be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation in Assert for both parameters)
  - bitmapword (type used for word comparison)
- Called from (representative examples):
  - [append_total_cost_compare](../a/append_total_cost_compare.md)
  - [append_startup_cost_compare](../a/append_startup_cost_compare.md)

## Notes and Other Information
- Returns 0 if sets are equal, positive if a > b, negative if a < b
- Compatible with qsort and other standard comparison-based sorting algorithms
- Guarantees consistency with bms_equal: returns 0 iff bms_equal would return true
- NULL is treated as the smallest possible set (less than any non-NULL set)
- Comparison is performed from most significant to least significant words for efficiency
- Used primarily in path comparison functions for query optimization
- The comparison rule makes intuitive sense: sets with higher-numbered bits are considered greater
- Validates both input sets in debug builds using Assert