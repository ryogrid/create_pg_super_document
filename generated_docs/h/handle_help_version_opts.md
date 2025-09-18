# handle_help_version_opts

## Location
src/fe_utils/option_utils.c: 24 - 49

## Overview
Provides standardized handling of --help and --version command-line options across PostgreSQL client utilities.

## Definition


## Detailed Description
This utility function implements consistent behavior for help and version options across all PostgreSQL frontend tools. When invoked, it checks if the first command-line argument is a help or version request, and if so, displays the appropriate information and exits. This ensures all PostgreSQL client programs respond uniformly to these standard options.

The function checks for both long and short forms of the options:
- Help:  or 
- Version:  or 

When a help option is detected, it calls the provided help handler function. For version requests, it prints the program name along with the PostgreSQL version and exits.

## Parameters / Member Variables
- : Number of command-line arguments
- : Array of command-line argument strings
- : The canonical name of the program to display in version output
- : Function pointer to the help handler that displays usage information

## Dependencies
- Functions called/Symbols referenced:
  - get_progname
  - strcmp
  - printf
  - exit
  - PG_VERSION (macro)
- Called from (representative examples):
  - main (in pg_amcheck)
  - main (in pg_combinebackup) 
  - main (in clusterdb)
  - main (in createdb)
  - main (in various other client utilities)

## Notes and Other Information
- This function should be called early in main() before other argument processing
- The help_handler type is typically a function that takes a program name and prints usage information
- The function only checks argv[1], so it should be called before any argument reordering
- Uses exit(0) for both help and version, providing clean termination
- Part of the fe_utils library for frontend utility functions