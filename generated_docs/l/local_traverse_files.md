# local_traverse_files

## Location
src/bin/pg_rewind/local_source.c: 60 - 65

## Overview
A static function that implements the traverse_files operation for local sources by calling traverse_datadir on the local data directory.

## Definition
static void local_traverse_files(rewind_source *source, process_file_callback_t callback)

## Detailed Description
This function serves as the local implementation of the traverse_files method in the rewind_source interface. It extracts the data directory path from the local_source structure and delegates the actual directory traversal to the traverse_datadir function, passing along the provided callback function. This allows pg_rewind to enumerate all files in the local PostgreSQL data directory using a uniform interface.

The function acts as a thin wrapper that adapts the generic rewind_source interface to the specific traverse_datadir implementation used for local file system operations.

## Parameters / Member Variables
- `source`: Pointer to the rewind_source structure (cast to local_source internally)
- `callback`: Function pointer of type process_file_callback_t that will be called for each file found during traversal

## Dependencies
- Functions called/Symbols referenced:
  - [traverse_datadir](../t/traverse_datadir.md)
- Called from (representative examples):
  - Via function pointer in rewind_source interface

## Notes and Other Information
- This function is static and only used within the local_source.c file
- It's assigned to the traverse_files function pointer in init_local_source
- The function performs a type cast from rewind_source* to local_source* to access the datadir member
- Part of the strategy pattern implementation that allows pg_rewind to handle both local and remote sources uniformly
- Located in src/bin/pg_rewind/local_source.c:60-65