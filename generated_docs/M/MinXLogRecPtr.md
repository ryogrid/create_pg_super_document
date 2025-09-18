# MinXLogRecPtr

## Location
src/bin/pg_rewind/pg_rewind.c: 842 - 855

## Overview
MinXLogRecPtr is a utility function that finds the minimum WAL location from two XLogRecPtr values, treating InvalidXLogRecPtr as infinity for timeline-related comparisons.

## Definition


## Detailed Description
This function implements a specialized comparison for WAL (Write-Ahead Log) locations that handles invalid pointers according to timeline semantics. Unlike a standard minimum function, it treats InvalidXLogRecPtr as representing infinity, which is consistent with the semantics defined in src/include/access/timeline.h. This behavior is specifically designed for comparing WAL locations related to history files during timeline operations.

The function uses a three-way conditional logic:
1. If the first parameter is invalid, return the second parameter
2. If the second parameter is invalid, return the first parameter  
3. If both parameters are valid, return the standard minimum

## Parameters / Member Variables
- : First XLogRecPtr value to compare
- : Second XLogRecPtr value to compare

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid (macro to check if XLogRecPtr is invalid)
  - Min (standard minimum macro)
- Called from (representative examples):
  - findCommonAncestorTimeline

## Notes and Other Information
- This is a static function local to pg_rewind.c
- Specifically designed for timeline history file operations where InvalidXLogRecPtr represents infinity
- Should only be used when comparing WAL locations related to history files
- Part of the pg_rewind utility which synchronizes a PostgreSQL data directory with another copy of the same directory