# isolation_init

## Location
src/test/isolation/isolation_main.c: 111 - 136

## Overview
Initializes the isolation testing framework by saving the executable path and setting up the default database configuration.

## Definition


## Detailed Description
This function performs essential initialization for the isolation testing framework. It saves a copy of argv[0] for later use in locating the isolationtester binary, since the binary lookup cannot be performed during initialization due to library search path constraints. The function also establishes the default regression database name that will be used for isolation tests. The delayed binary lookup strategy is necessary because regression_main() calls initialization functions before parsing command line arguments, which means the library search path hasn't been properly configured yet.

## Parameters / Member Variables
- : Command line argument count (currently unused in function body)
- : Command line argument array, used to extract argv[0] for executable path

## Dependencies
- Functions called/Symbols referenced:
  - strlcpy
  - add_stringlist_item
- Called from (representative examples):
  - main

## Notes and Other Information
- Cannot perform find_other_exec() lookup during initialization due to timing constraints with library search path setup
- Saves argv[0] in saved_argv0 global variable for later use by isolation_start_test()
- Sets "isolation_regression" as the default database name for tests
- Validates that the executable path length doesn't exceed MAXPGPATH
- Part of PostgreSQL's isolation testing framework initialization sequence