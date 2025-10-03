# table_rescan_tidrange

## Location
[src/include/access/tableam.h:1106-1121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1106-L1121)

## Overview
Resets the scan position and sets new minimum and maximum TID range boundaries for an existing TID range scan.

## Definition
```c
static inline void
table_rescan_tidrange(TableScanDesc sscan, ItemPointer mintid,
                      ItemPointer maxtid)
```

## Detailed Description
This function resets an existing TID range scan to restart from the beginning with a potentially new TID range. It first validates that the provided scan descriptor was created for TID range scanning by checking for the SO_TYPE_TIDRANGESCAN flag. The function then performs a general scan reset and reconfigures the TID range boundaries using the table access method's scan_set_tidrange function.

This is typically used when the same scan needs to be reused with different TID range parameters or when restarting a scan from the beginning.

## Parameters / Member Variables
- `sscan`: The existing TableScanDesc that was created by table_beginscan_tidrange
- `mintid`: Pointer to the new minimum TID (starting point of the range)
- `maxtid`: Pointer to the new maximum TID (ending point of the range)

## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDesc](../T/TableScanDesc.md) (parameter type)
  - SO_TYPE_TIDRANGESCAN (validation flag)
  - Assert (validation macro)
  - sscan->rs_rd->rd_tableam->scan_rescan (table access method function)
  - sscan->rs_rd->rd_tableam->scan_set_tidrange (table access method function)
- Called from (representative examples):
  - [TidRangeNext](../T/TidRangeNext.md)

## Notes and Other Information
- This is an inline function defined in the table access method header
- Includes an assertion to ensure the scan was created specifically for TID range scanning
- The scan_rescan call uses NULL parameters and all boolean flags set to false for a clean reset
- Used for restarting TID range scans with potentially different range boundaries
- Essential for reusable scan operations in the TID range scan executor node

## Simplified Source

```c
static inline void
table_rescan_tidrange(TableScanDesc sscan, ItemPointer mintid,
                     ItemPointer maxtid)
{
    // Validate this is a TID range scan
    Assert((sscan->rs_flags & SO_TYPE_TIDRANGESCAN) != 0);

    // Reset the scan to beginning
    sscan->rs_rd->rd_tableam->scan_rescan(sscan, NULL, false, false, false, false);

    // Set new TID range boundaries
    sscan->rs_rd->rd_tableam->scan_set_tidrange(sscan, mintid, maxtid);
}
```