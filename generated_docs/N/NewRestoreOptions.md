# NewRestoreOptions

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1090 - 1106

## Overview
NewRestoreOptions is a factory function that allocates and initializes a new RestoreOptions structure with default values for PostgreSQL backup restoration operations.

## Definition


## Detailed Description
This function serves as a constructor for the RestoreOptions structure, which contains all configuration parameters and flags that control the behavior of the pg_restore process. It allocates memory using pg_malloc0 (which zeros the allocated memory) and then sets specific fields that require non-zero default values.

The function ensures consistent initialization of restore options across different entry points in the pg_dump/pg_restore utilities. By using pg_malloc0, most boolean flags and numeric fields are automatically initialized to false/0, while only specific fields that need different default values are explicitly set.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0 (allocates zero-initialized memory)
  - RestoreOptions (structure type)
  - archUnknown (format default)
  - TRI_DEFAULT (tri-state password prompt default)
  - DUMP_UNSECTIONED (dump sections default)
  - PG_COMPRESSION_NONE (compression algorithm default)
- Called from (representative examples):
  - main (in pg_dump.c)
  - main (in pg_restore.c)
  - _CloseArchive

## Notes and Other Information
- Returns a heap-allocated RestoreOptions structure that must be freed by the caller
- Uses pg_malloc0 to ensure all fields start with zero/false values
- Sets non-zero defaults for:
  - format: archUnknown (archive format unknown initially)
  - cparams.promptPassword: TRI_DEFAULT (prompt for password behavior)
  - dumpSections: DUMP_UNSECTIONED (dump all sections by default)
  - compression_spec.algorithm: PG_COMPRESSION_NONE (no compression)
  - compression_spec.level: 0 (compression level zero)
- Designed for future expansion where additional initialization might be needed
- Critical for proper initialization of restore behavior across pg_dump utilities