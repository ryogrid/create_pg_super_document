# pgfnames_cleanup

## Location
src/common/pgfnames.c: 86 - 94

## Overview
Deallocates memory used by the filename array returned from pgfnames function.

## Definition


## Detailed Description
The  function is a companion to  that properly deallocates all memory allocated by the pgfnames function. It iterates through the NULL-terminated array of filename strings, freeing each individual string using pfree(), then frees the array itself. This function ensures complete cleanup of the dynamic memory allocation performed by pgfnames.

## Parameters / Member Variables
- : A NULL-terminated array of char pointers returned by pgfnames that needs to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - pfree: Deallocates memory for individual filename strings and the array
- Called from (representative examples):
  - scan_available_timezones: Used in initdb after processing timezone directory listings

## Notes and Other Information
- Must be called after using pgfnames to prevent memory leaks
- Assumes the input array is NULL-terminated as returned by pgfnames
- Uses PostgreSQL's pfree memory management function
- Safe to call even if some strings in the array are NULL
- The function handles the complete cleanup of both individual strings and the array structure