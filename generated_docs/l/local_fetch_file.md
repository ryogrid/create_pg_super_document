# local_fetch_file

## Location
src/bin/pg_rewind/local_source.c: 66 - 76

## Overview
A static function that implements the fetch_file operation for local sources by reading the entire contents of a file into memory.

## Definition
static char *local_fetch_file(rewind_source *source, const char *path, size_t *filesize)

## Detailed Description
This function serves as the local implementation of the fetch_file method in the rewind_source interface. It extracts the data directory path from the local_source structure and delegates the actual file reading to the slurpFile function. The function reads the entire contents of a specified file from the local PostgreSQL data directory into memory and returns it as a buffer, along with the file size.

This function provides a uniform interface for reading complete files during the pg_rewind process, allowing the same code to work with both local and remote sources.

## Parameters / Member Variables
- `source`: Pointer to the rewind_source structure (cast to local_source internally)
- `path`: Relative path to the file within the data directory
- `filesize`: Pointer to size_t where the actual file size will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [slurpFile](../s/slurpFile.md)
- Called from (representative examples):
  - Via function pointer in rewind_source interface

## Notes and Other Information
- This function is static and only used within the local_source.c file
- It's assigned to the fetch_file function pointer in init_local_source
- The function performs a type cast from rewind_source* to local_source* to access the datadir member
- Returns a dynamically allocated buffer containing the file contents that must be freed by the caller
- Part of the strategy pattern implementation for uniform handling of local and remote file operations
- Located in src/bin/pg_rewind/local_source.c:66-76