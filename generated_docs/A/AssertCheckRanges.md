# AssertCheckRanges

## Location
src/backend/access/brin/brin_minmax_multi.c: 296 - 425

## Overview
AssertCheckRanges is a comprehensive debugging function that validates the internal consistency and invariants of a Ranges structure used in BRIN minmax-multi indexes.

## Definition


## Detailed Description
This function performs extensive validation of a Ranges structure, which is a core data structure in BRIN minmax-multi access method. The function verifies multiple critical invariants:

1. **Basic sanity checks**: Validates that counts are non-negative and relationships between nranges, nsorted, nvalues, and maxvalues are correct
2. **Range ordering**: Ensures that range boundary values are strictly ordered using AssertArrayOrder
3. **Point value ordering**: Validates that sorted single-point values maintain proper order
4. **Range coverage**: Verifies that no individual values fall within existing ranges (which would be redundant)
5. **Sorted vs unsorted separation**: Ensures values in the unsorted part don't duplicate values in the sorted part

The function uses binary search to efficiently check whether individual values are covered by existing ranges, and employs sophisticated validation logic to maintain the integrity of the compressed range representation.

## Parameters / Member Variables
- : Pointer to the Ranges structure to validate
- : FmgrInfo pointer to the comparison function for ordering operations
- : OID of the collation to use for comparison operations

## Dependencies
- Functions called/Symbols referenced:
  - AssertArrayOrder
  - FunctionCall2Coll
  - bsearch_arg
  - compare_values
- Data structures referenced:
  - Ranges
  - compare_context
- Called from (representative examples):
  - range_deduplicate_values
  - ensure_free_space_in_buffer
  - range_add_value
  - compactify_ranges

## Notes and Other Information
- This function only executes when USE_ASSERT_CHECKING is defined (debug builds)
- Implements sophisticated binary search logic to verify range coverage efficiently
- Critical for maintaining data integrity in BRIN minmax-multi indexes
- The function assumes ranges are stored as pairs of boundary values followed by individual point values
- Part of the comprehensive validation framework for BRIN index structures
- Located in src/backend/access/brin/brin_minmax_multi.c:296-425