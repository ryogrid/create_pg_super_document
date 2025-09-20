# NewDumpOptions

## Location
[src/bin/pg_dump/pg_backup_archiver.c:130-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L130-L141)

## Overview
Creates and initializes a new DumpOptions structure with all default values for PostgreSQL dump operations.

## Definition

```c
DumpOptions *
NewDumpOptions(void)
```
## Detailed Description
NewDumpOptions is a constructor function that allocates memory for a new DumpOptions structure and initializes it with default values. This function serves as the primary entry point for creating DumpOptions instances used throughout the pg_dump utility. The function combines memory allocation with initialization by calling InitDumpOptions to set up all fields with appropriate default values.

The DumpOptions structure contains numerous configuration options for controlling PostgreSQL dump behavior, including schema/data selection, output formatting, connection parameters, and various command-line flags.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc: Allocates memory for the DumpOptions structure
  - [InitDumpOptions](../I/InitDumpOptions.md): Initializes the newly allocated structure with default values
  - DumpOptions: The structure type being allocated
- Called from (representative examples):
  - [dumpOptionsFromRestoreOptions](../d/dumpOptionsFromRestoreOptions.md): Creates DumpOptions from RestoreOptions

## Notes and Other Information
- The function allocates exactly sizeof(DumpOptions) bytes of memory
- Memory allocation is done using pg_malloc, which handles allocation failures internally
- The returned pointer should be freed when no longer needed
- All initialization is delegated to InitDumpOptions for consistency
- This is the standard way to create DumpOptions instances in the pg_dump codebase