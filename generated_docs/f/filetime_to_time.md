# filetime_to_time

## Location
[src/port/win32stat.c:25-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32stat.c#L25-L47)

## Overview
Converts a Windows FILETIME structure to a 64-bit time_t value representing Unix epoch time.

## Definition

```c
static __time64_t
filetime_to_time(const FILETIME *ft)
```
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
  - [fileinfo_to_stat](fileinfo_to_stat.md) (multiple times at src/port/win32stat.c:86, 90, 96)

## Notes and Other Information
- This is a static function, only accessible within the win32stat.c file
- Returns -1 if the input FILETIME is before the Unix epoch (invalid)
- Uses EpochShift constant of 116444736000000000 to adjust between Windows and Unix epochs
- Part of PostgreSQL's Windows compatibility layer for file system operations

## Simplified Source

```c
static __time64_t
filetime_to_time(const FILETIME *ft)
{
    ULARGE_INTEGER unified_ft = {0};
    static const uint64 EpochShift = UINT64CONST(116444736000000000);

    // Combine low and high parts into 64-bit value
    unified_ft.LowPart = ft->dwLowDateTime;
    unified_ft.HighPart = ft->dwHighDateTime;

    // Check if before Unix epoch
    if (unified_ft.QuadPart < EpochShift)
        return -1;

    // Convert from Windows epoch (1601) to Unix epoch (1970)
    unified_ft.QuadPart -= EpochShift;

    // Convert from 100-nanosecond intervals to seconds
    unified_ft.QuadPart /= 10 * 1000 * 1000;

    return unified_ft.QuadPart;
}
```