# skip_drive

## Location
[src/port/path.c:68-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L68-L84)

## Overview
A static utility function that skips over the drive portion of a file path, handling both Windows drive letters and UNC paths.

## Definition
```c
static char *skip_drive(const char *path)
```

## Detailed Description
The skip_drive function is designed to handle platform-specific path prefixes by skipping over drive specifications in file paths. It handles two types of drive specifications:

1. **UNC paths**: Paths starting with two directory separators (e.g., "\\\\server\\share") - it skips past the server name portion
2. **Windows drive letters**: Paths with a drive letter followed by a colon (e.g., "C:") - it skips past the drive letter and colon

The function returns a pointer to the portion of the path after the drive specification, allowing subsequent path processing functions to work with the path component independent of the drive.

## Parameters / Member Variables
- `path`: Input file path string that may contain a drive specification

## Dependencies
- Functions called/Symbols referenced:
  - IS_DIR_SEP (macro for checking directory separators)
- Called from (representative examples):
  - [has_drive_prefix](../h/has_drive_prefix.md)
  - [first_dir_separator](../f/first_dir_separator.md)
  - [last_dir_separator](../l/last_dir_separator.md)
  - [join_path_components](../j/join_path_components.md)
  - [canonicalize_path_enc](../c/canonicalize_path_enc.md)
  - [path_contains_parent_reference](../p/path_contains_parent_reference.md)
  - [get_progname](../g/get_progname.md)
  - [trim_directory](../t/trim_directory.md)
  - [trim_trailing_separator](../t/trim_trailing_separator.md)

## Notes and Other Information
- This is a static function internal to src/port/path.c
- Handles cross-platform path compatibility by abstracting drive specifications
- Uses the IS_DIR_SEP macro to handle different directory separator characters across platforms
- For UNC paths, it skips past the server name but stops at the first directory separator after the server name
- Essential for path manipulation functions that need to work with the directory structure independent of drive specifications

## Simplified Source

```c
// Simplified version of skip_drive
static char *skip_drive(const char *path) {
    // Handle UNC paths (\\server\share)
    if (IS_DIR_SEP(path[0]) && IS_DIR_SEP(path[1])) {
        // Skip past the initial double separators
        path += 2;

        // Skip past the server name
        while (*path && !IS_DIR_SEP(*path)) {
            path++;
        }
    }
    // Handle Windows drive letters (C:)
    else if (isalpha((unsigned char) path[0]) && path[1] == ':') {
        // Skip past drive letter and colon
        path += 2;
    }

    // Return pointer past the drive specification
    return (char *) path;
}
```

Key simplifications made:
- Added clear comments for both UNC and drive letter handling
- Preserved the essential dual path format support
- Maintained the cross-platform compatibility logic
- Kept the important pointer advancement for each case