# path_is_relative_and_below_cwd

## Location
[src/port/path.c:603-635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L603-L635)

## Overview
Determines whether a canonicalized path is relative and guaranteed to remain within or below the current working directory.

## Definition


## Detailed Description
The  function performs a security-focused validation to determine if a path is both relative and safe (i.e., cannot escape the current working directory). This function is critical for preventing directory traversal attacks.

The function performs several checks:

1. **Absolute path check**: Returns false for absolute paths since we only want paths relative to the current working directory
2. **Parent reference check**: Returns false if the path contains any '..' components that could allow escaping the current directory
3. **Windows drive specification check**: On Windows, rejects drive-relative paths like 'E:abc' which are relative to the current directory on a specific drive, as their behavior is unpredictable

The function ensures that only truly safe relative paths that remain within the current working directory subtree are accepted.

## Parameters / Member Variables
- : A null-terminated string containing the canonicalized file system path to be validated. The path must have been previously processed by canonicalize_path functions.

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  -  (Windows-specific macro)
- Called from (representative examples):
  -  (src/backend/utils/adt/genfile.c:86)

## Notes and Other Information
- **Critical requirement**: The input path MUST have been processed through  or  beforehand
- This function is a key security mechanism for preventing directory traversal attacks in PostgreSQL
- The Windows-specific logic handles the ambiguous case of drive-relative paths (like 'E:abc') by conservatively rejecting them
- Drive-relative paths are rejected because their actual target depends on the current directory state of the specified drive, which could change unpredictably
- Returns true only for relative paths that are guaranteed to stay within the current working directory tree
- Commonly used in file access functions to validate user-provided paths before allowing file operations