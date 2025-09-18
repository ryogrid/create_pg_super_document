# filetime_to_time

## Location
src/port/win32stat.c: 25 - 47

## Overview
Converts a Windows FILETIME structure to a 64-bit time_t value representing Unix epoch time.

## Definition


## Detailed Description
This function converts Windows FILETIME values to Unix time_t format. FILETIME represents the number of 100-nanosecond intervals since January 1, 1601 UTC, while Unix time_t represents seconds since January 1, 1970 UTC. The function performs the necessary epoch shift and time unit conversion to bridge these two time representations.

The conversion process involves:
1. Combining the low and high parts of FILETIME into a single 64-bit value
2. Subtracting the epoch difference (116444736000000000 represents the 100-nanosecond intervals between 1601 and 1970)
3. Converting from 100-nanosecond intervals to seconds by dividing by 10,000,000

## Parameters / Member Variables
- `ft`: Pointer to a Windows FILETIME structure containing the time to be converted

## Dependencies
- Functions called/Symbols referenced:
  - FILETIME (Windows API structure)
  - ULARGE_INTEGER (Windows API union)
  - UINT64CONST macro
- Called from:
  - fileinfo_to_stat (multiple times at src/port/win32stat.c:86, 90, 96)

## Notes and Other Information
- This is a static function, only accessible within the win32stat.c file
- Returns -1 if the input FILETIME is before the Unix epoch (invalid)
- Uses EpochShift constant of 116444736000000000 to adjust between Windows and Unix epochs
- Part of PostgreSQL's Windows compatibility layer for file system operations