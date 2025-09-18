# scan_available_timezones

## Location
[src/bin/initdb/findtimezone.c:657-1564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/findtimezone.c#L657-L1564)

## Overview
Recursively scans the timezone database directory looking for the best match to the system timezone behavior, comparing timezone files against system timezone characteristics.

## Definition


## Detailed Description
This function implements a recursive directory traversal algorithm to find the timezone file that best matches the system's timezone behavior. It operates by:

1. Scanning all entries in the current timezone directory using 
2. For each entry, determining if it's a subdirectory or timezone file
3. Recursively processing subdirectories to explore the full timezone hierarchy
4. For timezone files, calculating a match score using 
5. Maintaining the best match found so far, with tie-breaking logic based on timezone name preferences and alphabetical ordering

The function modifies the  buffer during traversal but restores it to its original state before returning, ensuring the caller's buffer remains intact.

## Parameters / Member Variables
- : Buffer of size MAXPGPATH containing the pathname of a directory with TZ files; modified internally but restored on exit
- : Points to the subfile name portion of tzdir (original directory name length + 1 for '/')
- : Pointer to tztry struct containing system timezone behavior data that needs to be matched
- : Pointer to integer holding the best match score found so far; updated if a better score is found
- : Buffer of length TZ_STRLEN_MAX + 1 containing the name of the best timezone found; updated with better matches

## Dependencies
- Functions called/Symbols referenced:
  - pgfnames: Get list of files in directory
  - S_ISDIR: Check if path is directory
  - [score_timezone](score_timezone.md): Calculate match score for timezone file
  - [zone_name_pref](../z/zone_name_pref.md): Get timezone name preference ranking
  - strlcpy: Safe string copy
  - pgfnames_cleanup: Clean up file list
  - TZ_STRLEN_MAX: Maximum timezone string length constant
- Called from:
  - [identify_system_timezone](../i/identify_system_timezone.md): Main timezone identification function
  - [scan_available_timezones](scan_available_timezones.md): Recursive calls for subdirectories

## Notes and Other Information
- Uses recursive directory traversal to explore the complete timezone database hierarchy
- Implements sophisticated tie-breaking logic when multiple timezones have equal scores, preferring zones with higher name preference rankings, shorter names, or lexicographically smaller names
- Includes debug output capability when DEBUG_IDENTIFY_TIMEZONE is defined
- Handles file system errors gracefully by continuing to process remaining entries
- Critical component of PostgreSQL's timezone auto-detection system during database initialization