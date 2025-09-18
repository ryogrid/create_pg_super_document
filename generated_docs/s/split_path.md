# split_path

## Location
src/bin/pg_waldump/pg_waldump.c: 161 - 187

## Overview
A utility function that splits a file path into directory and filename components, similar to the Unix dirname(1) and basename(1) commands.

## Definition
```c
static void split_path(const char *path, char **dir, char **fname)
```

## Detailed Description
The split_path function parses a file path and separates it into its directory and filename components. It searches for the last occurrence of the '/' character using strrchr() to find the directory separator. If a separator is found, the function allocates memory for the directory portion (using pnstrdup) and the filename portion (using pg_strdup). If no separator is found, it assumes the path refers to a file in the current directory, setting the directory pointer to NULL and duplicating the entire path as the filename. The function handles memory allocation for both output parameters, making the caller responsible for freeing the allocated memory.

## Parameters / Member Variables
- `path`: Input null-terminated string containing the file path to be split
- `dir`: Output parameter - pointer to char pointer that will receive the allocated directory path (may be NULL)
- `fname`: Output parameter - pointer to char pointer that will receive the allocated filename

## Dependencies
- Functions called/Symbols referenced:
  - strrchr (standard C library function to find last occurrence of character)
  - pnstrdup (PostgreSQL utility function to duplicate a specified number of characters)
  - pg_strdup (PostgreSQL utility function to duplicate a string)
- Called from (representative examples):
  - main (called in pg_waldump.c:1120)
  - main (called in pg_waldump.c:1159)

## Notes and Other Information
- The function has known limitations on Windows platforms and may need canonicalize_path() preprocessing
- Memory allocated for both dir and fname must be freed by the caller using pg_free()
- Uses Unix-style '/' path separator, which may not be appropriate for all platforms
- If the path ends with '/', the filename will be an empty string
- The directory component does not include the trailing '/' separator
- Returns NULL for directory when the path contains no directory separators (local file)