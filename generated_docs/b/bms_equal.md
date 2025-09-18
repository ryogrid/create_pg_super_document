# bms_equal

## Location
src/backend/nodes/bitmapset.c: 142 - 182

## Overview
Compares two Bitmapsets for equality, returning true if they contain exactly the same set of bits or if both are NULL.

## Definition
bool bms_equal(const Bitmapset *a, const Bitmapset *b)

## Detailed Description
bms_equal performs a comprehensive equality comparison between two Bitmapsets. The function implements a multi-stage comparison process:

1. **NULL handling**: Both NULL sets are considered equal (representing empty sets), while one NULL and one non-NULL are not equal
2. **Word count comparison**: Sets with different numbers of words cannot be equal
3. **Bitwise comparison**: Each word in the bitmap is compared for exact matches

The function efficiently determines equality by first checking metadata (word counts) before performing the more expensive bitwise comparison. This approach optimizes performance for obviously unequal sets while ensuring accurate results for potentially equal sets.

## Parameters / Member Variables
- `a`: A constant pointer to the first Bitmapset to compare. Can be NULL.
- `b`: A constant pointer to the second Bitmapset to compare. Can be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_valid_set (validation in Assert for both parameters)
- Called from (representative examples):
  - AlterPublicationTables
  - bitmap_match
  - _equalBitmapset
  - merge_clump
  - make_one_rel
  - set_rel_pathlist
  - add_paths_to_append_rel
  - get_cheapest_parameterized_child_path
  - standard_join_search
  - choose_bitmap_and
  - join_is_legal
  - populate_joinrel_with_paths
  - try_partitionwise_join
  - add_path_precheck
  - create_append_path
  - create_merge_append_path
  - find_join_rel

## Notes and Other Information
- Returns true if both parameters are NULL (empty sets are equal)
- Returns false if only one parameter is NULL
- Uses efficient early termination when word counts differ
- Validates both input sets in debug builds using Assert
- Widely used throughout PostgreSQL for comparing sets of relation IDs, attribute numbers, and other bitmap-represented collections
- Essential for optimization decisions in query planning and execution
- Part of the core equality checking infrastructure used by the node equality system