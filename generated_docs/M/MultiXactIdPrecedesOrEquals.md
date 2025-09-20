# MultiXactIdPrecedesOrEquals

## Location
[src/backend/access/transam/multixact.c:3323-3334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3323-L3334)

## Overview
MultiXactIdPrecedesOrEquals determines whether one MultiXactId is logically less than or equal to another, handling wrap-around behavior in the MultiXact ID space.

## Definition

```c
bool
MultiXactIdPrecedesOrEquals(MultiXactId multi1, MultiXactId multi2)
```
## Detailed Description
This function implements a precedence or equality comparison for MultiXact IDs using modular arithmetic to handle wrap-around. It computes the difference between the two MultiXact IDs as a signed 32-bit integer and returns true if multi1 precedes or equals multi2 (multi1 <= multi2). Like MultiXactIdPrecedes, it assumes that MultiXact IDs are close enough in value that the difference fits within the range of a 32-bit signed integer.

This function is used in various PostgreSQL subsystems for determining age relationships between MultiXact IDs, particularly in vacuum operations, heap tuple processing, and MultiXact truncation logic. The "or equals" aspect is important for boundary conditions and inclusive range checks.

## Parameters / Member Variables
- : First MultiXact ID to compare (MultiXactId)
- : Second MultiXact ID to compare (MultiXactId)

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactId (type)
- Called from (representative examples):
  - [heap_tuple_should_freeze](../h/heap_tuple_should_freeze.md)
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [TruncateMultiXact](../T/TruncateMultiXact.md)
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md)

## Notes and Other Information
- The function uses signed 32-bit arithmetic to handle wrap-around in the MultiXact ID space
- Returns true if multi1 is earlier than or equal to multi2
- The implementation assumes MultiXact IDs being compared are within 2^31 of each other
- Similar to MultiXactIdPrecedes but includes equality in the comparison (<=)
- There's a comment suggesting potential special handling for InvalidMultiXactId might be needed, but current implementation treats it normally
- This is a public function used in vacuum operations and MultiXact management
- Essential for determining inclusive bounds in MultiXact processing and cleanup operations