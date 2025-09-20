# handle_help_version_opts

## Location
[src/fe_utils/option_utils.c:24-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/option_utils.c#L24-L49)

## Overview
Provides standardized handling of --help and --version command-line options across PostgreSQL client utilities.

## Definition

```c
void
handle_help_version_opts(int argc, char *argv[],
						 const char *fixed_progname, help_handler hlp)
```
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
  - [get_progname](../g/get_progname.md)
  - strcmp
  - printf
  - exit
  - PG_VERSION (macro)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_amcheck)
  - [main](../m/main.md) (in pg_combinebackup) 
  - [main](../m/main.md) (in clusterdb)
  - [main](../m/main.md) (in createdb)
  - [main](../m/main.md) (in various other client utilities)

## Notes and Other Information
- This function should be called early in main() before other argument processing
- The help_handler type is typically a function that takes a program name and prints usage information
- The function only checks argv[1], so it should be called before any argument reordering
- Uses exit(0) for both help and version, providing clean termination
- Part of the fe_utils library for frontend utility functions