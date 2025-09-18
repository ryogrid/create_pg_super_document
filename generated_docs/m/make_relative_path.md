# make_relative_path

## Location
[src/port/path.c:737-805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L737-L805)

## Overview
Creates a relative path from a target directory to support relocation of PostgreSQL installation trees by computing paths based on the actual executable location.

## Definition


## Detailed Description
The  function is a core component of PostgreSQL's installation relocation support. It allows PostgreSQL installations to be moved to different directories while maintaining correct relative paths between components.

The function works by:
1. Finding the common prefix between the compiled-in target path and bin path
2. Extracting the remainder of the bin path (the "tail")
3. Checking if this tail matches the corresponding part of the actual executable path
4. If matched, constructing a new path by replacing the common prefix with the actual installation prefix
5. If no match, falling back to the original target path

For example, if PostgreSQL was compiled with  as the bin directory and  as the share directory, but is actually installed in , the function will correctly map the share directory to .

## Parameters / Member Variables
- : Output buffer (must be MAXPGPATH size) to store the resulting relative path
- : The compiled-in path to the directory we want to find (e.g., share directory)
- : The compiled-in path to the directory of executables
- : The actual location of the current executable

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to check directory separators
  -  - Safe string copying function
  -  - Removes the last path component
  -  - Normalizes path format
  -  - Directory-aware string comparison
  -  - Removes trailing path separators
  -  - Safely joins path components

- Called from (representative examples):
  -  - Getting shared data directory
  -  - Getting configuration directory
  -  - Getting header file directory
  -  - Getting library directory
  - Various other  functions for different PostgreSQL directories

## Notes and Other Information
- This is a static function, only accessible within src/port/path.c
- Critical for PostgreSQL's portability and ability to run from relocated installations
- Handles cross-platform path differences through the use of directory-aware helper functions
- Falls back gracefully to the original compiled-in path if relocation logic fails
- Used extensively by PostgreSQL utilities to find installation directories relative to the executable location
- The algorithm requires that the common prefix ends on a directory separator to avoid partial directory name matches