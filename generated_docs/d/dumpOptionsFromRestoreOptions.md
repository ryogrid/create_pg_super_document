# dumpOptionsFromRestoreOptions

## Location
src/bin/pg_dump/pg_backup_archiver.c: 156 - 209

## Overview
Creates a new DumpOptions structure with options equivalent to those found in a given RestoreOptions structure, enabling conversion between restore and dump configurations.

## Definition
```c
DumpOptions *dumpOptionsFromRestoreOptions(RestoreOptions *ropt)
```

## Detailed Description
This function serves as a bridge between restore and dump operations by converting RestoreOptions into equivalent DumpOptions. It performs the inverse operation of what happens at the end of pg_dump.c's main() function. The function creates a fresh DumpOptions structure and maps all relevant fields from the RestoreOptions parameter.

The conversion handles connection parameters, output formatting options, data/schema selection flags, and various behavioral settings. String fields are deep-copied using pg_strdup to ensure the DumpOptions structure owns its memory, while scalar values are copied directly.

This functionality is crucial for operations that need to convert restore-time settings back into dump-compatible format, maintaining consistency across the dump/restore cycle.

## Parameters / Member Variables
- `ropt`: Pointer to the RestoreOptions structure to convert from

## Dependencies
- Functions called/Symbols referenced:
  - [NewDumpOptions](../N/NewDumpOptions.md): Creates and initializes a new DumpOptions structure
  - [pg_strdup](../p/pg_strdup.md): Creates duplicate copies of string fields
  - DumpOptions: The target structure type
  - [RestoreOptions](../R/RestoreOptions.md): The source structure type
- Called from (representative examples):
  - [SetArchiveOptions](../S/SetArchiveOptions.md): Uses this function to set up archive options from restore options

## Notes and Other Information
- The function performs deep copying of string fields (dbname, pgport, pghost, username, superuser, restrict_key) to ensure memory ownership
- NULL string fields are preserved as NULL in the target structure
- The mapping preserves the semantic meaning of options between dump and restore contexts
- Connection parameters (cparams) are mapped field-by-field rather than as a whole structure
- Output formatting flags are directly mapped (outputClean ← dropSchema, etc.)
- The function is described as performing 'the inverse of what's at the end of pg_dump.c's main()'
- Memory allocated by this function (and NewDumpOptions) should be properly freed when no longer needed