# XLogDumpDisplayStats

## Location
src/bin/pg_waldump/pg_waldump.c: 626 - 755

## Overview
A comprehensive statistics display function in pg_waldump that formats and prints detailed WAL record analysis statistics, showing counts and sizes by resource manager and optionally by individual record types.

## Definition


## Detailed Description
XLogDumpDisplayStats generates a formatted statistical report of WAL records processed during pg_waldump analysis. The function first calculates totals across all resource managers, then displays statistics in a tabular format showing record counts, record sizes, full page image (FPI) sizes, and combined sizes with percentages. It supports two display modes: by resource manager only, or with detailed breakdown by individual record types within each resource manager. The output includes column headers, individual rows for each category, separator lines, and a summary total row with percentage breakdowns.

## Parameters / Member Variables
- : Configuration object containing display options and settings for the waldump analysis
- : Statistics structure containing accumulated counts and sizes for WAL records processed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid: Checks if LSN pointer is invalid
  - RmgrIdIsValid: Validates resource manager ID
  - [GetRmgrDesc](../G/GetRmgrDesc.md): Retrieves resource manager descriptor
  - [RmgrIdIsCustom](../R/RmgrIdIsCustom.md): Checks if resource manager is custom
  - [XLogDumpStatsRow](XLogDumpStatsRow.md): Displays individual statistic rows
  - printf: Standard output formatting
  - [psprintf](../p/psprintf.md): PostgreSQL string formatting function
  - LSN_FORMAT_ARGS: Macro for LSN formatting
  - INT64_MODIFIER: Platform-specific 64-bit integer format modifier
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function, only accessible within pg_waldump.c
- Early returns if no statistics have been computed (tracked by invalid endptr)
- Uses two-pass approach: first calculates totals, then displays individual rows with percentages
- Handles both aggregate (by resource manager) and detailed (by record type) display modes
- The detailed mode uses rm_identify callbacks to get human-readable record type names
- Column widths are carefully calculated for consistent tabular formatting
- Shows LSN range being analyzed in the header
- Final totals show percentages of record vs FPI data within the total combined size
- Skips custom resource managers that have zero records to reduce noise