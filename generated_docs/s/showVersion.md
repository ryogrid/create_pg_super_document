# showVersion

## Location
src/bin/psql/startup.c: 839 - 857

## Overview
A static utility function that displays the psql version information in a format compatible with GNU standards.

## Definition


## Detailed Description
This function provides a simple, standardized way to display the psql version information. It outputs the program name along with the PostgreSQL version using the PG_VERSION macro. The output format is specifically designed to match GNU standards for version display, making it consistent with other GNU utilities and expected by automated tools that parse version information.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - puts (standard C library function for string output)
  - PG_VERSION (PostgreSQL version macro)
- Called from (representative examples):
  - [adhoc_opts](../a/adhoc_opts.md)
  - [main](../m/main.md)
  - [parse_psql_options](../p/parse_psql_options.md)

## Notes and Other Information
- The output format follows GNU standards for consistency with other command-line tools
- Uses the PG_VERSION compile-time constant to ensure version accuracy
- This is typically invoked when the --version or -V command-line option is specified
- The function has no return value and performs direct console output
- Simple implementation focused solely on version display without additional information