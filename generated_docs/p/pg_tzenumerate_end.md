# pg_tzenumerate_end

## Location
src/timezone/pgtz.c: 414 - 425

## Overview
Cleans up and deallocates resources used by timezone enumeration, closing all open directory descriptors and freeing memory allocated for the pg_tzenum structure.

## Definition
```c
void pg_tzenumerate_end(pg_tzenum *dir)
```

## Detailed Description
This function performs cleanup for timezone enumeration by properly releasing all resources that were allocated during the enumeration process. It iterates through all levels of the directory traversal stack (from current depth down to 0), closing each open directory descriptor and freeing the associated directory name strings.

The function ensures that no directory handles are left open and no memory is leaked by systematically cleaning up each level of the nested directory traversal that may have occurred during timezone enumeration. After cleaning up all the nested resources, it frees the main pg_tzenum structure itself.

## Parameters / Member Variables
- `dir`: Pointer to the pg_tzenum structure to be cleaned up and freed

## Dependencies
- Functions called/Symbols referenced:
  - pg_tzenum (structure type being cleaned up)
  - FreeDir (PostgreSQL function to close directory descriptors)
  - [pfree](pfree.md) (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - [pg_timezone_names](pg_timezone_names.md) (in datetime.c after completing timezone enumeration)

## Notes and Other Information
- Must be called to properly clean up resources created by pg_tzenumerate_start()
- Handles cleanup for nested directory traversal by working backwards through the depth stack
- Uses PostgreSQL's memory management (pfree) rather than standard free()
- Essential for preventing memory leaks and file descriptor leaks during timezone enumeration
- Part of the timezone enumeration trilogy: start, next, and end functions
- Safe to call even if enumeration was terminated early before completing all timezones
- The depth field tracks how many directory levels are currently open for cleanup