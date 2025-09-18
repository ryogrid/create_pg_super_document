# range_add_value

## Location
[src/backend/access/brin/brin_minmax_multi.c:1702-1787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1702-L1787)

## Overview
This function adds a new value to a BRIN minmax-multi range structure, managing space allocation, deduplication, and maintaining sorted order.

## Definition


## Detailed Description
The function implements a comprehensive strategy for adding new values to BRIN minmax-multi ranges. It first ensures adequate buffer space by calling ensure_free_space_in_buffer, which may trigger compaction and deduplication. Before actually adding the value, it checks if the value is already covered by existing ranges to avoid duplicates.

If the value is new, it makes a copy of the value (respecting the attribute's pass-by-value semantics) and adds it to the values array. The function maintains the invariant that values should be kept sorted for efficient searches, though it uses a simple insertion approach rather than complex sorting algorithms given the typically small maxvalues (e.g., 64).

The function includes extensive assertions to verify the integrity of the range structure both before and after the operation, ensuring that the newly added value is properly contained within the ranges.

## Parameters / Member Variables
- : BRIN descriptor containing index metadata and operator procedures
- : Collation OID used for value comparisons
- : Attribute number being indexed
- : Form_pg_attribute structure containing attribute metadata (type, length, pass-by-value flag)
- : Ranges structure to be modified with the new value
- : Datum value to be added to the ranges

## Dependencies
- Functions called/Symbols referenced:
  - [minmax_multi_get_strategy_procinfo](../m/minmax_multi_get_strategy_procinfo.md)
  - [AssertCheckRanges](../A/AssertCheckRanges.md)
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [range_contains_value](range_contains_value.md)
  - [datumCopy](../d/datumCopy.md)
- Called from:
  - [brin_minmax_multi_add_value](../b/brin_minmax_multi_add_value.md)

## Notes and Other Information
- Returns true if the range was modified (either by space management or value addition), false if no changes occurred
- Performs duplicate checking before adding values to prevent redundant storage
- The function prioritizes early duplicate detection over batch processing for efficiency reasons
- Maintains sorted order in the values array using simple insertion rather than complex sorting algorithms
- Uses datumCopy to properly handle both pass-by-value and pass-by-reference data types
- Critical entry point for value insertion in BRIN minmax-multi indexes, ensuring data integrity through comprehensive validation
- The space management occurs before containment checking to handle values that might exist in the unsorted portion