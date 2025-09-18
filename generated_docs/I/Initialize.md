# Initialize

## Location
src/bin/pg_archivecleanup/pg_archivecleanup.c: 57 - 74

## Overview
Initializes the pg_archivecleanup program by validating that the specified archive location exists and is accessible as a directory.

## Definition
```c
static void Initialize(void)
```

## Detailed Description
The Initialize function serves as the initialization routine for the pg_archivecleanup utility. It performs essential validation of the archive location specified by the user to ensure the program can operate correctly. The function uses the stat() system call to verify that the archive location exists and is indeed a directory rather than a regular file or other file system object.

This function is part of the "Customizable section" of pg_archivecleanup, designed to allow users to extend the program with custom initialization logic for different archive storage backends (such as tape libraries or vendor-specific archive APIs).

## Parameters / Member Variables
This function takes no parameters and operates on the global variable:
- `archiveLocation`: Global string variable containing the path to the archive directory that needs validation

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md) (system call)
  - S_ISDIR (macro for checking directory type)
  - pg_log_error (PostgreSQL logging function)
  - exit (standard library function)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_archivecleanup/pg_archivecleanup.c:380)

## Notes and Other Information
- The function is marked as `static`, making it internal to the pg_archivecleanup.c file
- On failure (non-existent or non-directory archive location), the program terminates with exit code 2
- The function is designed as an extension point where additional initialization logic can be added for different archive storage systems
- This validation step prevents the program from attempting cleanup operations on invalid or inaccessible locations
- Located at src/bin/pg_archivecleanup/pg_archivecleanup.c:57-74