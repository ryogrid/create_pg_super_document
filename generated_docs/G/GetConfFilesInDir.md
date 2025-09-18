# GetConfFilesInDir

## Location
src/backend/utils/misc/conffiles.c: 70 - 164

## Overview
Returns an alphabetically sorted list of configuration files (ending in ".conf") found in a specified directory, with comprehensive error handling and validation.

## Definition
```c
char **GetConfFilesInDir(const char *includedir, const char *calling_file, 
                        int elevel, int *num_filenames, char **err_msg)
```

## Detailed Description
This function scans a directory for configuration files and returns them as an array of absolute file paths. It implements several important behaviors:
- Only includes files with ".conf" extension and at least 6 characters in length
- Explicitly excludes hidden files (starting with ".") and system entries like "." and ".."
- Converts the directory path to absolute using AbsoluteConfigLocation
- Sorts results alphabetically for consistent processing order
- Provides detailed error reporting for various failure conditions
- Uses dynamic memory allocation, growing the array in blocks of 32 entries

The function validates the input directory name to prevent empty or blank-only names that could cause recursive inclusion issues.

## Parameters / Member Variables
- `includedir`: The directory path to scan for configuration files
- `calling_file`: The file that requested this directory scan (used for relative path resolution)
- `elevel`: Error reporting level for ereport() calls
- `num_filenames`: Output parameter returning the number of files found
- `err_msg`: Output parameter for detailed error message on failure

## Dependencies
- Functions called/Symbols referenced:
  - AbsoluteConfigLocation
  - AllocateDir
  - ReadDir
  - FreeDir
  - join_path_components
  - canonicalize_path
  - get_dirent_type
  - qsort
  - pg_qsort_strcmp
  - palloc/repalloc/pstrdup
  - ereport/errcode/errmsg
- Called from (representative examples):
  - tokenize_auth_file (hba.c)

## Notes and Other Information
- Returns NULL on error with details in err_msg parameter
- Caller is responsible for freeing the returned array and all contained strings
- Uses PostgreSQL's memory allocation functions (palloc/repalloc) for automatic cleanup
- Part of PostgreSQL's configuration file inclusion system, particularly for processing include_dir directives
- Implements robust error handling with both ereport() logging and caller-visible error messages
- The 6-character minimum length requirement effectively enforces the ".conf" extension while allowing for at least one character in the base filename