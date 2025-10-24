# pgfnames_cleanup

## Location
[src/common/pgfnames.c:86-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pgfnames.c#L86-L94)

## Overview
Deallocates memory used by the filename array returned from pgfnames function.

## Definition

```c
void
pgfnames_cleanup(char **filenames)
```
## Detailed Description
The  function is a companion to  that properly deallocates all memory allocated by the pgfnames function. It iterates through the NULL-terminated array of filename strings, freeing each individual string using pfree(), then frees the array itself. This function ensures complete cleanup of the dynamic memory allocation performed by pgfnames.

## Parameters / Member Variables
- `**filenames`: A NULL-terminated array of char pointers returned by pgfnames that needs to be deallocated
## Dependencies
- Functions called/Symbols referenced:
  - [pfree](pfree.md): Deallocates memory for individual filename strings and the array
- Called from (representative examples):
  - [scan_available_timezones](../s/scan_available_timezones.md): Used in initdb after processing timezone directory listings

## Notes and Other Information
- Must be called after using pgfnames to prevent memory leaks
- Assumes the input array is NULL-terminated as returned by pgfnames
- Uses PostgreSQL's pfree memory management function
- Safe to call even if some strings in the array are NULL
- The function handles the complete cleanup of both individual strings and the array structure

## Simplified Source

```c
void pgfnames_cleanup(char **filenames) {
    // Free each filename string
    for (char **fn = filenames; *fn; fn++) {
        pfree(*fn);
    }

    // Free the array itself
    pfree(filenames);
}
```