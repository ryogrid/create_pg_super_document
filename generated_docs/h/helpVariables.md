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
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize the output buffer)
  - HELP0/HELPN (macros for adding help text)
  - DEFAULT_FIELD_SEP (default field separator constant)
  - [PageOutput](../P/PageOutput.md) (handle paginated output)
  - [ClosePager](../C/ClosePager.md) (close the pager when done)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup the buffer)
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

## Simplified Source

```c
void helpVariables(unsigned short int pager) {
    PQExpBufferData buf;
    int nlcount;
    FILE *output;

    // Build help text in buffer for line counting and pagination
    initPQExpBuffer(&buf);

    // Add psql variables section
    HELP0("List of specially treated variables\n\n");
    HELP0("psql variables:\n");
    HELP0("Usage:\n  psql --set=NAME=VALUE\n  or \\set NAME VALUE inside psql\n\n");

    // Document each major psql variable with description
    HELP0("  AUTOCOMMIT\n    if set, successful SQL commands are automatically committed\n");
    HELP0("  ECHO\n    controls what input is written to standard output\n    [all, errors, none, queries]\n");
    HELP0("  ENCODING\n    current client character set encoding\n");
    HELP0("  ERROR\n    \"true\" if last query failed, else \"false\"\n");
    // ... continues with all psql variables ...

    // Add display settings section
    HELP0("\nDisplay settings:\n");
    HELP0("Usage:\n  psql --pset=NAME[=VALUE]\n  or \\pset NAME [VALUE] inside psql\n\n");

    // Document display formatting options
    HELP0("  format\n    set output format [unaligned, aligned, wrapped, html, asciidoc, ...]\n");
    HELP0("  expanded (or x)\n    expanded output [on, off, auto]\n");
    // ... continues with all display settings ...

    // Add environment variables section
    HELP0("\nEnvironment variables:\n");
    HELP0("Usage:\n");
    #ifndef WIN32
        HELP0("  NAME=VALUE [NAME=VALUE] psql ...\n  or \\setenv NAME [VALUE] inside psql\n\n");
    #else
        HELP0("  set NAME=VALUE\n  psql ...\n  or \\setenv NAME [VALUE] inside psql\n\n");
    #endif

    // Document key environment variables
    HELP0("  PGDATABASE\n    same as the dbname connection parameter\n");
    HELP0("  PGHOST\n    same as the host connection parameter\n");
    // ... continues with all environment variables ...

    // Count newlines for pagination
    nlcount = 0;
    for (const char *ptr = buf.data; *ptr; ptr++) {
        if (*ptr == '\n')
            nlcount++;
    }

    // Display with pagination if requested
    output = PageOutput(nlcount, pager ? &(pset.popt.topt) : NULL);
    fputs(buf.data, output);
    ClosePager(output);

    termPQExpBuffer(&buf);
}
```