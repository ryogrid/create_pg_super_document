# line_starts_with

## Location
src/bin/pg_combinebackup/backup_label.c: 224 - 240

## Overview
A static utility function that tests whether a line of text starts with a specified prefix string and optionally returns a pointer to the position immediately after the match.

## Definition


## Detailed Description
The  function performs a prefix match operation on a line of text bounded by start and end pointers. It compares the beginning of the line against a provided match string character by character. The function is designed to work with non-null-terminated strings by using explicit boundaries.

The function performs simultaneous iteration through both the line content and the match string, advancing both pointers while characters match. It returns true if the entire match string is found at the beginning of the line, and false otherwise. When a successful match occurs and an output parameter is provided, it stores a pointer to the character immediately following the matched prefix.

This function is essential for parsing backup label files where specific line prefixes need to be identified (such as "START WAL LOCATION:", "INCREMENTAL FROM LSN:", etc.) and the remaining line content needs to be processed.

## Parameters / Member Variables
- : Pointer to the start of the line to test
- : Pointer to the end of the line (exclusive boundary - points one past the last character)
- : Null-terminated string to match against the beginning of the line
- : Optional output parameter to store pointer to the position after the match (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic string operations)
- Called from (representative examples):
  - [parse_backup_label](../p/parse_backup_label.md) (multiple times for different line types)
  - [write_backup_label](../w/write_backup_label.md) (for filtering incremental backup lines)

## Notes and Other Information
- Static function scope limits visibility to the backup_label.c source file
- Safe for use with non-null-terminated strings due to explicit boundary checking
- Returns true only when the entire match string is found at the line start
- The sout parameter provides a convenient way to get the position after the prefix for further parsing
- Used extensively in backup label parsing to identify different types of configuration lines
- Does not modify any of the input strings - purely a read-only operation