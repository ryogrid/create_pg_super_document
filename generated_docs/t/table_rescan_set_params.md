# table_rescan_set_params

## Location
src/include/access/tableam.h: 1044 - 1055

## Overview
Restarts a table scan with the ability to modify scan parameters including buffer strategy, synchronized scanning, and page mode options.

## Definition


## Detailed Description
The  function provides an advanced way to restart a table scan while allowing modification of important scan parameters that affect performance and behavior. Unlike the basic  function, this variant enables changing buffer strategy, synchronized scanning, and page mode settings before restarting the scan.

This function is particularly useful for operations that need to adjust scan behavior based on runtime conditions or when the scan requirements change during execution. The function preserves the previously selected start block even when sync scan settings are modified, ensuring consistent scanning behavior.

## Parameters / Member Variables
- : The TableScanDesc structure representing the scan to be restarted
- : Optional new scan key data to apply during the rescan (can be NULL)
- : Whether to allow buffer strategy changes
- : Whether to allow synchronized scanning changes
- : Whether to allow page mode changes

## Dependencies
- Functions called/Symbols referenced:
  - TableScanDesc (scan descriptor type)
  - ScanKeyData (scan key structure type)
  - scan->rs_rd->rd_tableam->scan_rescan (table access method rescan function with parameters)
- Called from (representative examples):
  - tablesample_init (src/backend/executor/nodeSamplescan.c:304)

## Notes and Other Information
- This is an inline function defined in the table access method header file
- More flexible than basic table_rescan as it allows parameter modification
- The function calls scan_rescan with the first boolean parameter set to true to indicate parameter changes
- Buffer strategy affects how pages are managed in the buffer pool during scanning
- Synchronized scanning coordinates multiple scans to reduce I/O by scanning similar blocks together
- Page mode controls how individual pages are processed during the scan
- Used primarily in specialized scan operations like table sampling
- Part of PostgreSQL's table access method (TAM) abstraction layer
- Preserves start block selection even when sync scan behavior changes