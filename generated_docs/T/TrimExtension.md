# TrimExtension

## Location
[src/bin/pg_archivecleanup/pg_archivecleanup.c:75-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_archivecleanup/pg_archivecleanup.c#L75-L90)

## Overview
Removes a specified extension from the end of a filename string by truncating the filename in-place.

## Definition
```c
static void TrimExtension(char *filename, char *extension)
```

## Detailed Description
The TrimExtension function modifies a filename string by removing a specified extension from its end. It performs this operation in-place by null-terminating the filename string at the appropriate position. The function first validates that both the filename and extension are long enough for the extension to be present at the end of the filename, then performs a string comparison to ensure the extension matches exactly before removing it.

This utility function is commonly used in pg_archivecleanup to normalize WAL filenames by removing additional extensions that may have been added during archiving processes.

## Parameters / Member Variables
- `filename`: Pointer to the filename string that will be modified in-place. The string must be mutable and null-terminated.
- `extension`: Pointer to the extension string to be removed from the filename. If NULL, the function returns immediately without modifications.

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard library function)
  - strcmp (standard library function)
- Called from (representative examples):
  - [CleanupPriorWALFiles](../C/CleanupPriorWALFiles.md) (in src/bin/pg_archivecleanup/pg_archivecleanup.c:115)
  - [SetWALFileNameForCleanup](../S/SetWALFileNameForCleanup.md) (in src/bin/pg_archivecleanup/pg_archivecleanup.c:187)

## Notes and Other Information
- The function is marked as `static`, making it internal to the pg_archivecleanup.c file
- The function safely handles the case where extension is NULL by returning early
- The extension is only removed if it exactly matches the end of the filename (case-sensitive)
- The filename length must be greater than the extension length for removal to occur
- The modification is performed in-place by setting a null terminator, which permanently shortens the original string
- Located at src/bin/pg_archivecleanup/pg_archivecleanup.c:75-90