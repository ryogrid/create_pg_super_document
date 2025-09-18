# has_drive_prefix

## Location
src/port/path.c: 93 - 108

## Overview
A platform-specific utility function that determines whether a given pathname contains a drive prefix (Windows drive letters or UNC paths).

## Definition
```c
bool has_drive_prefix(const char *path)
```

## Detailed Description
The has_drive_prefix function provides a cross-platform way to detect whether a file path contains a drive specification. The implementation is conditionally compiled based on the target platform:

- **On Windows (WIN32)**: Uses the skip_drive function to determine if a drive prefix exists by checking if skip_drive returns a different pointer than the input path
- **On non-Windows platforms**: Always returns false since drive prefixes are not used on Unix-like systems

This function is essential for path processing logic that needs to behave differently depending on whether the path is absolute with a drive specification or relative/absolute without one.

## Parameters / Member Variables
- `path`: Input file path string to check for drive prefix presence

## Dependencies
- Functions called/Symbols referenced:
  - [skip_drive](../s/skip_drive.md) (on Windows platforms only)
- Called from (representative examples):
  - [process_file](../p/process_file.md) (in src/bin/psql/command.c)

## Notes and Other Information
- This is a public function declared in the port header files
- Provides platform abstraction for drive prefix detection
- On Windows, detects both traditional drive letters (C:) and UNC paths (\\\\server\\share)
- On Unix-like systems, consistently returns false since drive concepts don't exist
- Essential for cross-platform path handling in PostgreSQL utilities
- Used by psql and other tools that need to handle file paths differently based on drive presence