# DistanceValue

## Location
[src/backend/access/brin/brin_minmax_multi.c:248-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L248-L252)

## Overview
DistanceValue is a structure that represents the distance between two ranges in PostgreSQL's BRIN minmax_multi index access method, identified by index into an array of extended ranges.

## Definition


## Detailed Description
DistanceValue is a lightweight data structure used within PostgreSQL's BRIN (Block Range Index) minmax_multi access method to facilitate range optimization operations. It stores a calculated distance metric between two ranges along with the index that identifies one of the ranges in an array of extended ranges. This structure is primarily used during range consolidation and compaction processes where the system needs to determine which ranges are closest to each other for merging or optimization purposes.

The structure is defined in the BRIN minmax_multi implementation and serves as a building block for algorithms that need to make distance-based decisions about range management, such as reducing the number of ranges while maintaining index effectiveness.

## Parameters / Member Variables
- : Integer identifier that references a specific range within an array of extended ranges
- : Double-precision floating-point number representing the calculated distance metric between two ranges

## Dependencies
- Functions called/Symbols referenced: (None - this is a simple data structure)
- Called from (representative examples):
  - [compare_distances](../c/compare_distances.md)
  - [build_distances](../b/build_distances.md)
  - [reduce_expanded_ranges](../r/reduce_expanded_ranges.md)
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [compactify_ranges](../c/compactify_ranges.md)
  - [brin_minmax_multi_union](../b/brin_minmax_multi_union.md)

## Notes and Other Information
- This structure is specific to the BRIN minmax_multi access method implementation
- The distance value is typically calculated using range-specific metrics that determine how "close" two ranges are to each other
- Used primarily in internal algorithms for range optimization and is not exposed to end users
- The structure is designed for efficiency in sorting and comparison operations during range management