# point_eq_point

## Location
[src/backend/utils/adt/geo_ops.c:1977-1992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1977-L1992)

## Overview
Internal static function that performs the core logic for comparing two points for equality, handling both regular floating-point values and NaN cases.

## Definition


## Detailed Description
The  function is the fundamental comparison function for Point equality in PostgreSQL's geometric system. It implements a two-tier comparison strategy: for normal floating-point values, it uses tolerance-based comparison through , but when NaN values are involved, it requires exact bitwise equality using . This approach ensures consistent and predictable behavior across all possible Point values while maintaining proper floating-point semantics.

## Parameters / Member Variables
- : Pointer to the first Point structure to compare
- : Pointer to the second Point structure to compare

## Dependencies
- Functions called/Symbols referenced:
  -  - checks if floating-point values are NaN
  -  - exact floating-point equality comparison 
  -  - floating-point equality with tolerance
- Called from (representative examples):
  -  - SQL-callable point equality operator
  -  - SQL-callable point inequality operator
  -  - box equality checking
  -  - line segment equality
  -  - circle equality
  -  - point list comparison

## Notes and Other Information
- Marked as  for performance optimization in frequently called geometric operations
- Handles special case of NaN values by requiring exact equality rather than tolerance-based comparison
- Central to many geometric operations including box, line segment, circle, and polygon comparisons
- Uses PostgreSQL's standard floating-point comparison utilities for consistent behavior
- The NaN handling ensures IEEE 754 compliance and predictable behavior in edge cases