# make_absolute_path

## Location
[src/port/path.c:806-900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L806-L900)

## Overview
Converts a relative path to an absolute path by resolving it against the current working directory and canonicalizing the result.

## Definition

```c
char *
make_absolute_path(const char *path)
```
## Detailed Description
The  function ensures that a given pathname is converted to an absolute path. If the input path is already absolute, it simply duplicates and canonicalizes it. If the path is relative, the function resolves it against the current working directory to create an absolute path.

Key features:
- Handles null input gracefully by returning NULL
- Uses  to determine if conversion is needed
- For relative paths, gets the current working directory using 
- Implements dynamic buffer allocation that grows if the working directory path is too long
- Handles memory allocation failures differently for backend vs frontend code
- Always canonicalizes the final path to ensure consistent formatting
- Returns a malloc'd copy that the caller must free

The function includes robust error handling for both memory allocation failures and  failures, with different error reporting mechanisms for backend (ereport/elog) and frontend (fprintf) contexts.

## Parameters / Member Variables
- : The input pathname that may be relative or absolute, or NULL

## Dependencies
- Functions called/Symbols referenced:
  -  - Determines if a path is already absolute
  -  - Dynamic memory allocation
  -  - Gets current working directory
  -  - [String](../S/String.md) duplication
  -  - [Path](../P/Path.md) normalization
  -  - Memory deallocation
  -  - [String](../S/String.md) formatting
  - [Backend](../B/Backend.md)-specific: ,  for error reporting
  - Frontend-specific:  for error output

- Called from (representative examples):
  -  - Setting PostgreSQL data directory
  -  - Configuration file path resolution
  -  - Windows command line processing
  -  - Test framework path setup

## Notes and Other Information
- Returns a malloc'd string that must be freed by the caller
- Gracefully handles NULL input by returning NULL
- Implements different error handling strategies for backend vs frontend compilation
- Critical for ensuring paths are properly resolved before directory changes (especially before )
- Uses dynamic buffer allocation for  to handle very long directory paths
- The canonicalization step ensures consistent path format across platforms
- Essential for configuration file processing and data directory management in PostgreSQL