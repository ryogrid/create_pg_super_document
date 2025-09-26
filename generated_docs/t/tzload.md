# tzload

## Location
src/timezone/localtime.c: 586 - 601

## Overview
The  function loads timezone data from a specified file into a timezone state structure, providing a wrapper around  with memory management.

## Definition


## Detailed Description
The  function serves as a memory-managed wrapper for loading timezone data. It allocates a temporary local storage buffer, calls the core  function to perform the actual timezone data loading, and then properly cleans up the allocated memory. The function supports both standard and extended timezone format loading based on the  parameter. If a  buffer is provided, it will store the canonical spelling of the timezone name upon successful loading.

## Parameters / Member Variables
- : The name of the timezone file to load (e.g., "America/New_York")
- : Optional buffer to store the canonical spelling of the timezone name (must be > TZ_STRLEN_MAX bytes if provided, can be NULL)
- : Pointer to the timezone state structure to populate with loaded data
- : Boolean flag indicating whether to read extended timezone format

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - local_storage (union type)
  - tzloadbody
  - free
- Called from (representative examples):
  - pg_load_tz
  - gmtload
  - pg_tzset
  - pg_tzenumerate_next

## Notes and Other Information
- Returns 0 on success, an errno value on failure
- Handles memory allocation failure by returning errno
- Acts as a memory management wrapper around tzloadbody
- Part of PostgreSQL's timezone handling system
- The function ensures proper cleanup of allocated memory regardless of success or failure of the underlying tzloadbody operation