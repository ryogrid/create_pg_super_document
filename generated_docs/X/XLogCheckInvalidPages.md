# XLogCheckInvalidPages

## Location
src/backend/access/transam/xlogutils.c: 245 - 313

## Overview
Checks for and reports any remaining invalid page entries in the invalid page hash table, typically called during recovery consistency checking to ensure all WAL references to invalid pages have been resolved.

## Definition


## Detailed Description
This function iterates through the global  hash table to identify any remaining invalid page entries that haven't been resolved during WAL recovery. It employs a two-phase reporting strategy: first emitting WARNING messages for all remaining invalid entries to provide comprehensive diagnostic information, then issuing either a WARNING or PANIC depending on the  setting.

The function serves as a final validation step in WAL recovery, ensuring that all page references in the WAL stream correspond to valid, accessible pages. If invalid pages remain, it indicates potential data corruption or incomplete recovery.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - hash_seq_init
  - hash_seq_search
  - report_invalid_page
  - hash_destroy
  - elog
- Data structures used:
  - HASH_SEQ_STATUS
  - xl_invalid_page
  - invalid_page_tab (global hash table)
- Called from:
  - CheckRecoveryConsistency (src/backend/access/transam/xlogrecovery.c:2235)

## Notes and Other Information
- The function uses a sequential scan approach to report all invalid pages before taking any fatal action
- Behavior is controlled by the  configuration parameter
- When  is true, invalid pages generate warnings instead of panic
- The invalid_page_tab hash table is destroyed and reset to NULL after processing
- This function is typically called near the end of recovery to ensure data consistency