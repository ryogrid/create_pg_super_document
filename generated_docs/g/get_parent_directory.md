# get_parent_directory

## Location
[src/port/path.c:1053-1069](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L1053-L1069)

## Overview
Modifies a file path string in-place to obtain the parent directory of the specified file or directory.

## Definition

```c
void
get_parent_directory(char *path)
```
## Detailed Description
This function takes a file path and modifies it in-place to represent the parent directory of the original path. It serves as a simple wrapper around the  function. The function is designed to work with file paths where the next operation will typically be .

Important behavioral notes: If the input is just a filename with no directory component, the result will be an empty string rather than ".". The function may not produce desirable results if the input string ends with "..", so callers should consider applying  first when dealing with potentially non-canonical paths.

## Parameters / Member Variables
- : Input/output string containing the file path to be modified in-place (must be writable)

## Dependencies
- Functions called/Symbols referenced:
  - [trim_directory](../t/trim_directory.md)
- Called from (representative examples):
  - [dbase_redo](../d/dbase_redo.md)
  - [destroy_tablespace_directories](../d/destroy_tablespace_directories.md)  
  - [fsync_parent_path](../f/fsync_parent_path.md)
  - [AbsoluteConfigLocation](../A/AbsoluteConfigLocation.md)
  - [main](../m/main.md) (initdb)
  - [process_file](../p/process_file.md)

## Notes and Other Information
- This is a void function that modifies the input string in-place
- The input string must be mutable (not a string literal)
- For inputs that are just filenames without directory components, returns empty string, not "."
- May produce unexpected results with paths ending in ".." - consider using canonicalize_path() first
- Primarily intended for use cases where the result will be passed to join_path_components()
- Used extensively throughout PostgreSQL for path manipulation operations