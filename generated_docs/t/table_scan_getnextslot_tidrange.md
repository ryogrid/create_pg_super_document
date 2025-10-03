# table_scan_getnextslot_tidrange

## Location
[src/include/access/tableam.h:1122-1174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1122-L1174)

## Overview
Fetches the next tuple from a TID range scan and stores it in the provided slot, returning true if a tuple was found or false if no more tuples exist in the range.

## Definition
```c
static inline bool
table_scan_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
                                TupleTableSlot *slot)
```

## Detailed Description
This function retrieves the next tuple from an active TID range scan created by table_beginscan_tidrange(). It validates that the scan descriptor is configured for TID range scanning and supports both forward and backward scan directions. The function delegates to the table access method's scan_getnextslot_tidrange implementation to perform the actual tuple retrieval and slot population.

The function includes assertions to ensure proper usage: the scan must be a TID range scan and the direction must be either forward or backward (NoMovementScanDirection is not supported for actual scanning operations).

## Parameters / Member Variables
- `sscan`: The TableScanDesc created by table_beginscan_tidrange
- `direction`: The scan direction (ForwardScanDirection or BackwardScanDirection)
- `slot`: The TupleTableSlot to store the retrieved tuple

## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDesc](../T/TableScanDesc.md) (parameter type)
  - ScanDirection (parameter type)  
  - [TupleTableSlot](../T/TupleTableSlot.md) (parameter type)
  - SO_TYPE_TIDRANGESCAN (validation flag)
  - ForwardScanDirection (direction constant)
  - BackwardScanDirection (direction constant)
  - Assert (validation macro)
  - sscan->rs_rd->rd_tableam->scan_getnextslot_tidrange (table access method function)
- Called from (representative examples):
  - [TidRangeNext](../T/TidRangeNext.md)

## Notes and Other Information
- This is an inline function defined in the table access method header
- Returns a boolean indicating whether a tuple was successfully retrieved
- Includes validation assertions for scan type and scan direction
- NoMovementScanDirection is explicitly not supported for actual tuple retrieval
- Used as the core tuple fetching mechanism in TID range scan execution
- The actual tuple retrieval logic is delegated to the table access method implementation

## Simplified Source

```c
static inline bool
table_scan_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
                                TupleTableSlot *slot)
{
    // Validate this is a TID range scan
    Assert((sscan->rs_flags & SO_TYPE_TIDRANGESCAN) != 0);

    // Validate scan direction (forward or backward only)
    Assert(direction == ForwardScanDirection ||
           direction == BackwardScanDirection);

    // Delegate to table access method to fetch next tuple
    return sscan->rs_rd->rd_tableam->scan_getnextslot_tidrange(sscan,
                                                              direction,
                                                              slot);
}
```