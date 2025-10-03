# MultiXactIdPrecedes

## Location
[src/backend/access/transam/multixact.c:3309-3322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3309-L3322)

## Overview
MultiXactIdPrecedes determines which of two MultiXactId values is earlier, accounting for wrap-around behavior in the MultiXact ID space.

## Definition

```c
bool
MultiXactIdPrecedes(MultiXactId multi1, MultiXactId multi2)
```
## Detailed Description
This function implements a precedence comparison for MultiXact IDs using modular arithmetic to handle wrap-around. It computes the difference between the two MultiXact IDs as a signed 32-bit integer and returns true if multi1 precedes multi2. The comparison assumes that MultiXact IDs are close enough in value that the difference fits within the range of a 32-bit signed integer, which is a standard technique for handling wrap-around in cyclic number spaces.

The function is fundamental to MultiXact management operations, including vacuum, freeze operations, and cleanup procedures. It's used throughout the system to determine the relative age of MultiXact IDs for various maintenance and consistency operations.

## Parameters / Member Variables
- `multi1`: First MultiXact ID to compare (MultiXactId)
- `multi2`: Second MultiXact ID to compare (MultiXactId)
## Dependencies
- Functions called/Symbols referenced:
  - MultiXactId (type)
- Called from (representative examples):
  - [FreezeMultiXactId](../F/FreezeMultiXactId.md)
  - [heap_prepare_freeze_tuple](../h/heap_prepare_freeze_tuple.md)
  - [heap_tuple_should_freeze](../h/heap_tuple_should_freeze.md)
  - [MultiXactIdSetOldestVisible](MultiXactIdSetOldestVisible.md)
  - [GetNewMultiXactId](../G/GetNewMultiXactId.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [SetMultiXactIdLimit](../S/SetMultiXactIdLimit.md)
  - [TruncateMultiXact](../T/TruncateMultiXact.md)
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md)
  - [vac_update_datfrozenxid](../v/vac_update_datfrozenxid.md)

## Notes and Other Information
- The function uses signed 32-bit arithmetic to handle wrap-around in the MultiXact ID space
- Returns true if multi1 is earlier (precedes) multi2
- The implementation assumes MultiXact IDs being compared are within 2^31 of each other
- There's a comment suggesting potential special handling for InvalidMultiXactId might be needed, but current implementation treats it normally
- This is a public function (not static) used extensively throughout the PostgreSQL codebase
- Critical for vacuum operations, tuple freezing, and MultiXact cleanup procedures

## Simplified Source

```c
// Simplified version of MultiXactIdPrecedes
bool MultiXactIdPrecedes(MultiXactId multi1, MultiXactId multi2) {
    // Calculate signed difference to handle wrap-around
    int32 diff = (int32) (multi1 - multi2);

    // Return true if multi1 comes before multi2
    return (diff < 0);
}
```

Key simplifications made:
- Added explanatory comments for the core algorithm
- Preserved the essential wrap-around handling logic using signed arithmetic
- Maintained the simple but critical comparison operation
- Removed XXX comment about InvalidMultiXactId handling as it's not implemented