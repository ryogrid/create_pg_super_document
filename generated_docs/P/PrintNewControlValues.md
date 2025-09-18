# PrintNewControlValues

## Location
src/bin/pg_resetwal/pg_resetwal.c: 789 - 860

## Overview
PrintNewControlValues displays the control file values that will be modified when pg_resetwal performs its reset operation.

## Definition


## Detailed Description
This static function is part of the pg_resetwal utility and is responsible for printing a formatted summary of all the control file values that will be changed during the WAL reset operation. The function conditionally prints various control file parameters based on what the user has requested to modify through command-line options. This provides transparency to the user about exactly what changes will be made before the actual reset occurs.

The function always prints the "First log segment after reset" information, and then conditionally prints other values only if they have been set through command-line options (checked via global variables like set_mxid, set_mxoff, set_oid, etc.).

## Parameters / Member Variables
This function takes no parameters and operates on global variables and the global ControlFile structure.

## Dependencies
- Functions called/Symbols referenced:
  - XLogFileName (to generate WAL segment filename)
  - XidFromFullTransactionId (to extract XID from full transaction ID)
  - EpochFromFullTransactionId (to extract epoch from full transaction ID)
  - MAXFNAMELEN (constant for maximum filename length)

- Called from:
  - main (in pg_resetwal.c at lines 464 and 474)

## Notes and Other Information
- This is a static function local to pg_resetwal.c
- The function uses internationalization macros (_()) for all printed strings
- Output is conditional based on global flags like set_mxid, set_mxoff, set_oid, set_xid, etc.
- Always prints the first log segment filename regardless of other settings
- Provides user-friendly output showing exactly what will change before the actual reset operation occurs
- Part of the pg_resetwal utility's user interface for transparency and confirmation