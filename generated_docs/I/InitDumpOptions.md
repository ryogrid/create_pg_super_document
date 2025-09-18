# InitDumpOptions

## Location
[src/bin/pg_dump/pg_backup_archiver.c:142-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L142-L155)

## Overview
Initializes a DumpOptions structure to all default values, setting up configuration for PostgreSQL dump operations.

## Definition
```c
void InitDumpOptions(DumpOptions *opts)
```

## Detailed Description
InitDumpOptions takes an existing DumpOptions structure and initializes all its fields to appropriate default values. The function first zeros out the entire structure using memset, then sets specific fields that shouldn't default to zero values. This ensures a clean, predictable initial state for dump operations.

The function is designed to be called on both newly allocated DumpOptions structures (via NewDumpOptions) and existing structures that need to be reset to defaults. It establishes the baseline configuration that can then be modified by command-line options or other configuration mechanisms.

## Parameters / Member Variables
- `opts`: Pointer to the DumpOptions structure to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - memset: Zeros out the entire structure
  - DumpOptions: The structure type being initialized
  - TRI_DEFAULT: Enum value for trivalue type (default password prompting behavior)
  - DUMP_UNSECTIONED: Constant (0xff) indicating unsectioned dump mode
- Called from (representative examples):
  - [NewDumpOptions](../N/NewDumpOptions.md): Initializes newly allocated DumpOptions
  - [main](../m/main.md): Direct initialization in pg_dump main function

## Notes and Other Information
- The function uses memset to zero the entire structure first, then sets non-zero defaults
- include_everything is set to true by default, meaning all database objects are included unless specifically excluded
- cparams.promptPassword is set to TRI_DEFAULT for default password prompting behavior
- dumpSections is set to DUMP_UNSECTIONED (0xff), indicating no specific section filtering
- This function establishes the baseline behavior that pg_dump uses when no specific options are provided
- All boolean flags default to false (via memset), which generally means features are disabled by default