# GetTempTablespaces

## Location
[src/backend/storage/file/fd.c:3090-3107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3090-L3107)

## Overview
Populates a caller-provided array with the tablespace OIDs configured for temporary file usage, returning the number of entries copied.

## Definition
```c
int GetTempTablespaces(Oid *tableSpaces, int numSpaces)
```

## Detailed Description
The `GetTempTablespaces` function provides access to the currently configured temporary tablespace list by copying the tablespace OIDs into a caller-provided array. This function is essential for components that need to retrieve the complete list of available temporary tablespaces for their own management purposes.

The function performs bounds checking to ensure that no more than `numSpaces` entries are copied, and no more entries are copied than are available in the configured temporary tablespace list. It returns the actual number of entries copied, which may be less than either the requested number or the total number of configured tablespaces.

The function includes an assertion to verify that temporary tablespaces have been properly configured before attempting to access them, ensuring system consistency and catching programming errors early.

## Parameters / Member Variables
- `tableSpaces`: Output array to receive the tablespace OIDs. Must be allocated by the caller with sufficient space
- `numSpaces`: Maximum number of entries to copy into the tableSpaces array

## Dependencies
- Functions called/Symbols referenced:
  - `TempTablespacesAreSet` - Verifies that temporary tablespaces are configured

- Global variables accessed:
  - `tempTableSpaces` - Array of configured temporary tablespace OIDs  
  - `numTempTableSpaces` - Count of configured temporary tablespaces

- Called from (representative examples):
  - `FileSetInit` (src/backend/storage/file/fileset.c:63)

## Notes and Other Information
- The function includes an assertion that temporary tablespaces are set, making it unsuitable for use when configuration is optional
- Some entries in the returned array may be InvalidOid, indicating the database's default tablespace should be used
- The caller is responsible for allocating sufficient space in the tableSpaces array
- Return value indicates the actual number of entries copied, which may be less than numSpaces
- Performs safe array copying with bounds checking to prevent buffer overflows
- Used primarily by subsystems that need to manage their own temporary file distribution across multiple tablespaces