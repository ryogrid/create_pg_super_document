# helpVariables

## Location
[src/bin/psql/help.c:361-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/help.c#L361-L573)

## Overview
The helpVariables function displays comprehensive documentation for all psql variables, display settings, and environment variables that control psql behavior.

## Definition
void helpVariables(unsigned short int pager)

## Detailed Description
This function generates and displays detailed help information about three categories of variables that affect psql operation: psql internal variables (set with \\set), display settings (configured with \\pset), and environment variables. It builds the complete help text in a buffer, counts lines for pagination, and displays the content appropriately. The help includes variable names, descriptions of their effects, possible values, and usage examples. Some sections include platform-specific content (e.g., Windows vs Unix environment variable syntax).

## Parameters / Member Variables
- `pager`: Controls whether the output should be paginated. Non-zero values enable pagination using psql's pager settings

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (buffer structure for building output)
  - initPQExpBuffer (initialize the output buffer)
  - HELP0/HELPN (macros for adding help text)
  - DEFAULT_FIELD_SEP (default field separator constant)
  - [PageOutput](../P/PageOutput.md) (handle paginated output)
  - [ClosePager](../C/ClosePager.md) (close the pager when done)
  - termPQExpBuffer (cleanup the buffer)
- Called from (representative examples):
  - [exec_command_slash_command_help](../e/exec_command_slash_command_help.md) (in src/bin/psql/command.c:3084)
  - [parse_psql_options](../p/parse_psql_options.md) (in src/bin/psql/startup.c:706)

## Notes and Other Information
- This function is part of psql's comprehensive help system, specifically for the \\? variables command
- Covers three main categories: psql variables, display settings, and environment variables
- Includes detailed descriptions of variable effects and accepted values
- Platform-specific help text sections using conditional compilation (WIN32 vs Unix)
- Documents both read-only variables (like ERROR, SQLSTATE) and user-configurable variables
- Provides usage examples for setting variables through command line and interactive commands
- Located at src/bin/psql/help.c:361-573
- The help includes all major psql configuration options from connection settings to output formatting