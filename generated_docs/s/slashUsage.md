# slashUsage

## Location
[src/bin/psql/help.c:151-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/help.c#L151-L360)

## Overview
The slashUsage function displays comprehensive help information for all psql backslash commands, organizing them into logical categories for easy reference.

## Definition
void slashUsage(unsigned short int pager)

## Detailed Description
This function generates and displays detailed help text for all psql backslash (meta) commands. It builds the complete help output in a buffer, counts the lines for pagination purposes, and then displays the content using the appropriate output method. The help is organized into logical sections including General commands, Help commands, Query Buffer operations, Input/Output, Conditional statements, Informational commands, Large Objects, Formatting options, Connection management, Operating System interactions, and Variables. Some help text includes dynamic content showing current settings (e.g., HTML mode status, timing status).

## Parameters / Member Variables
- `pager`: Controls whether the output should be paginated. Non-zero values enable pagination using psql's pager settings

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (buffer structure for building output)
  - [PQdb](../P/PQdb.md) (get current database name)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize the output buffer)
  - HELP0/HELPN (macros for adding help text)
  - ON (macro for displaying on/off status)
  - [PageOutput](../P/PageOutput.md) (handle paginated output)
  - [ClosePager](../C/ClosePager.md) (close the pager when done)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup the buffer)
  - Various pset variables for current settings display
- Called from (representative examples):
  - [exec_command_slash_command_help](../e/exec_command_slash_command_help.md) (in src/bin/psql/command.c:3080, 3086)
  - [parse_psql_options](../p/parse_psql_options.md) (in src/bin/psql/startup.c:704)

## Notes and Other Information
- This function is part of psql's interactive help system
- The help content includes dynamic elements that show current psql settings
- Uses internationalization support through gettext macros
- The function counts output lines to determine if pagination is needed
- Conditional compilation sections (e.g., USE_READLINE) affect which commands are displayed
- Located at src/bin/psql/help.c:151-360
- The output includes comprehensive coverage of all psql meta-commands with syntax and brief descriptions