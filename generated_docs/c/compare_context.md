# compare_context

## Location
src/backend/access/brin/brin_minmax_multi.c: 264 - 268

## Overview
compare_context is a structure that encapsulates comparison function information and collation settings for value comparison operations in PostgreSQL's BRIN minmax_multi index access method.

## Definition
```c
typedef struct compare_context
{
    FmgrInfo   *cmpFn;
    Oid         colloid;
} compare_context;
```

## Detailed Description
compare_context is a context structure used throughout PostgreSQL's BRIN minmax_multi access method to provide consistent comparison functionality for values within ranges. It encapsulates both the function manager information for the comparison function and the collation identifier, ensuring that all value comparisons within the index operations use the same comparison semantics and locale-specific sorting rules.

This structure is essential for maintaining data consistency and correctness when dealing with different data types and collations. It is passed to various functions that need to perform value comparisons, such as range deduplication, sorting, and containment checks. By centralizing the comparison context, the system ensures that all operations use the same comparison logic and collation settings.

## Parameters / Member Variables
- `cmpFn`: Pointer to FmgrInfo structure containing the comparison function information for the specific data type being indexed
- `colloid`: Object identifier (OID) specifying the collation to be used for string comparisons and sorting operations

## Dependencies
- Functions called/Symbols referenced: (None - this is a context structure)
- Called from (representative examples):
  - AssertCheckRanges
  - range_deduplicate_values
  - compare_expanded_ranges
  - compare_values
  - range_contains_value
  - sort_expanded_ranges
  - reduce_expanded_ranges

## Notes and Other Information
- This structure is specific to the BRIN minmax_multi access method implementation
- The FmgrInfo pointer typically points to a cached function call information structure to avoid repeated function lookups
- The collation OID is particularly important for text-based data types where locale-specific sorting rules apply
- Used internally within the BRIN implementation and not exposed to end users
- Ensures consistent comparison semantics across all range operations within a single index