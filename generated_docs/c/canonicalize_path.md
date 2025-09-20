# canonicalize_path

## Location
[src/port/path.c:336-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L336-L342)

## Overview
Cleans up and normalizes file system paths by applying various transformations to make paths consistent and canonical.

## Definition

```c
void
canonicalize_path(char *path)
```
## Detailed Description
The  function is a convenience wrapper around  that normalizes file system paths by applying multiple cleanup operations. It modifies the path in-place and performs the following transformations:

- Converts Win32 paths to use Unix-style forward slashes
- Removes trailing quotes on Win32 systems
- Removes trailing slashes from paths
- Removes duplicate adjacent path separators
- Removes '.' components (unless the path reduces to only '.')
- Processes '..' components, removing them when possible

This function assumes the input path is in a server-safe encoding and uses  as the encoding parameter when calling the underlying  function.

## Parameters / Member Variables
- : A null-terminated string containing the file system path to be canonicalized. The path is modified in-place.

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
- Called from (representative examples):
  -  (src/backend/commands/tablespace.c:236)
  -  (src/backend/commands/variable.c:1055)
  -  (src/backend/utils/adt/genfile.c:59)
  -  (src/common/exec.c:207)
  -  (src/port/path.c:767)
  -  (src/port/path.c:891)

## Notes and Other Information
- This function is part of PostgreSQL's portable path utilities located in src/port/path.c
- The function modifies the input string in-place, so callers should ensure they have a modifiable copy if the original path needs to be preserved
- This is the encoding-unaware variant - use  directly when dealing with paths that may contain characters in specific encodings
- Widely used throughout PostgreSQL codebase for path normalization in various contexts including tablespace management, configuration file handling, and executable path resolution