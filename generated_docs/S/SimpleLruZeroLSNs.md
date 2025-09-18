# SimpleLruZeroLSNs

## Location
src/backend/access/transam/slru.c: 428 - 444

## Overview
Zeros all LSN (Log Sequence Number) values stored for a specific SLRU page slot to ensure clean initialization.

## Definition


## Detailed Description
SimpleLruZeroLSNs is a static utility function that clears all LSN values associated with a particular SLRU page slot. This function is crucial for maintaining WAL (Write-Ahead Logging) consistency by ensuring that newly created pages or pages read from disk start with clean LSN state. The function operates under the assumption that InvalidXLogRecPtr is bitwise-all-0, allowing it to use a simple memory zeroing operation.

The function checks if the SLRU control structure has any LSN groups configured for the page, and if so, zeros out the entire LSN array for the specified slot. This prevents stale LSN values from interfering with WAL recovery and consistency checks.

## Parameters / Member Variables
- : SlruCtl structure containing the SLRU control information and shared state
- : The buffer slot number whose LSN values should be zeroed

## Dependencies
- Functions called/Symbols referenced:
  - MemSet (memory zeroing operation)
- Called from (representative examples):
  - SimpleLruZeroPage (when creating new pages)
  - SimpleLruReadPage (when reading pages from disk)

## Notes and Other Information
- This is a static function, only accessible within the slru.c file
- The function assumes InvalidXLogRecPtr is bitwise-all-0 for efficiency
- LSN zeroing is essential for both new page creation and disk page loading scenarios
- Only performs work if lsn_groups_per_page > 0, making it safe for SLRUs without LSN tracking
- The zeroing covers all LSN groups for the entire page slot
- This function helps maintain WAL consistency by ensuring clean LSN initialization