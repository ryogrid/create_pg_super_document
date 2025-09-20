# isolation_init

## Location
[src/test/isolation/isolation_main.c:111-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolation_main.c#L111-L136)

## Overview
Initializes the isolation testing framework by saving the executable path and setting up the default database configuration.

## Definition

```c
static void
isolation_init(int argc, char **argv)
```
## Detailed Description
This function performs essential initialization for the isolation testing framework. It saves a copy of argv[0] for later use in locating the isolationtester binary, since the binary lookup cannot be performed during initialization due to library search path constraints. The function also establishes the default regression database name that will be used for isolation tests. The delayed binary lookup strategy is necessary because regression_main() calls initialization functions before parsing command line arguments, which means the library search path hasn't been properly configured yet.

## Parameters / Member Variables
- : Command line argument count (currently unused in function body)
- : Command line argument array, used to extract argv[0] for executable path

## Dependencies
- Functions called/Symbols referenced:
  - strlcpy
  - [add_stringlist_item](../a/add_stringlist_item.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- Cannot perform find_other_exec() lookup during initialization due to timing constraints with library search path setup
- Saves argv[0] in saved_argv0 global variable for later use by isolation_start_test()
- Sets "isolation_regression" as the default database name for tests
- Validates that the executable path length doesn't exceed MAXPGPATH
- Part of PostgreSQL's isolation testing framework initialization sequence