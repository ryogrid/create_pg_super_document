# forkname_chars

## Location
src/common/relpath.c: 81 - 109

## Overview
Determines if a string begins with a valid PostgreSQL fork name and returns the length of the matching fork name prefix, helping identify relation fork files in database directories.

## Definition
```c
int forkname_chars(const char *str, ForkNumber *fork)
```

## Detailed Description
This function examines the beginning of a string to determine if it starts with a known fork name (excluding the main fork). It's primarily used for parsing filenames in database directories to distinguish relation fork files from other files. The function iterates through all non-main fork names, checking if the input string begins with any of them. If a match is found, it returns the length of the matching fork name and optionally sets the fork number. The function skips the main fork (forkNum = 0) since main fork files don't have a fork suffix.

## Parameters / Member Variables
- `str`: Input string to examine for fork name prefix
- `fork`: Optional output parameter to receive the matching ForkNumber (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - MAX_FORKNUM (maximum fork number constant)
  - forkNames (array of fork name strings)
  - strlen (standard C string length function)
  - strncmp (standard C string comparison function)
  - InvalidForkNumber (constant for invalid fork)
- Called from (representative examples):
  - looks_like_temp_rel_name (in src/backend/storage/file/fd.c:3470)
  - parse_filename_for_nontemp_relation (in src/backend/storage/file/reinit.c:422)
  - FORKNAMECHARS (referenced in src/include/common/relpath.h:69)

## Notes and Other Information
- Returns 0 if no fork name match is found at the beginning of the string
- Skips the main fork (index 0) since main fork files don't have fork suffixes
- Assumes no fork name is a prefix of another fork name
- Used for file system operations to identify relation fork files
- Part of PostgreSQL's relation file management and directory scanning functionality